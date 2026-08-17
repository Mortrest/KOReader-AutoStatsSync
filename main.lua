-- Automatic, append-only KOReader Reading Statistics synchronization.
--
-- This plugin deliberately reuses the WebDAV server and remote folder selected
-- under Reading statistics -> Cloud sync.  It replaces deletion propagation
-- with an append-only event merge so an incomplete device cannot erase history.

local DataStorage = require("datastorage")
local InfoMessage = require("ui/widget/infomessage")
local NetworkMgr = require("ui/network/manager")
local Notification = require("ui/widget/notification")
local SQ3 = require("lua-ljsqlite3/init")
local UIManager = require("ui/uimanager")
local WidgetContainer = require("ui/widget/container/widgetcontainer")
local ffiUtil = require("ffi/util")
local lfs = require("libs/libkoreader-lfs")
local logger = require("logger")
local util = require("util")
local _ = require("gettext")

local DB_PATH = DataStorage:getSettingsDir() .. "/statistics.sqlite3"
local BACKUP_DIR = DataStorage:getSettingsDir() .. "/autostatsync-backups"
local MAX_BACKUPS = 10
local OPEN_SYNC_DELAY_SECONDS = 3
local WATCHDOG_SECONDS = 120
local MAX_CONFLICT_RETRIES = 5

local AutoStatsSync = WidgetContainer:extend{
    name = "autostatsync",
    is_doc_only = true,
}

AutoStatsSync.default_settings = {
    enabled = true,
    sync_on_open = true,
    sync_on_close = true,
    only_when_online = true,
    last_status = _("Not synchronized yet"),
    last_attempt = 0,
}

local function sqlQuote(path)
    return "'" .. path:gsub("'", "''") .. "'"
end

local function databaseVersion(conn, database_name)
    local prefix = database_name and database_name .. "." or ""
    local ok, version = pcall(conn.rowexec, conn, "PRAGMA " .. prefix .. "user_version;")
    if not ok then
        return nil
    end
    return tonumber(version)
end

local function databaseIsValid(conn, database_name)
    local prefix = database_name and database_name .. "." or ""
    local ok, result = pcall(conn.rowexec, conn, "PRAGMA " .. prefix .. "quick_check;")
    return ok and result == "ok"
end

local function rotateBackups()
    local backups = {}
    for filename in lfs.dir(BACKUP_DIR) do
        if filename:match("^statistics%-%d%d%d%d%d%d%d%d%-%d%d%d%d%d%d.*%.sqlite3$") then
            table.insert(backups, filename)
        end
    end
    table.sort(backups)
    while #backups > MAX_BACKUPS do
        os.remove(BACKUP_DIR .. "/" .. table.remove(backups, 1))
    end
end

local function backupLocalDatabase()
    if lfs.attributes(BACKUP_DIR, "mode") ~= "directory" then
        local ok, err = lfs.mkdir(BACKUP_DIR)
        if not ok and lfs.attributes(BACKUP_DIR, "mode") ~= "directory" then
            logger.err("AutoStatsSync: cannot create backup directory:", err)
            return false
        end
    end

    local basename = os.date("statistics-%Y%m%d-%H%M%S")
    local backup_path = BACKUP_DIR .. "/" .. basename .. ".sqlite3"
    local suffix = 1
    while lfs.attributes(backup_path, "mode") == "file" do
        backup_path = BACKUP_DIR .. "/" .. basename .. "-" .. suffix .. ".sqlite3"
        suffix = suffix + 1
    end

    local copy_error = ffiUtil.copyFile(DB_PATH, backup_path)
    if copy_error ~= nil then
        logger.err("AutoStatsSync: failed to back up statistics database:", copy_error)
        return false
    end
    rotateBackups()
    return true
end

-- Callback signature required by cloudstorage.koplugin:sync().
-- cached_path is intentionally unused: this merge never propagates deletions.
function AutoStatsSync.mergeDatabases(local_path, _cached_path, income_path)
    local income = SQ3.open(income_path)
    local income_version = databaseVersion(income)
    if not income_version or income_version == 0 then
        -- A missing WebDAV file is represented by an empty download. Returning
        -- true makes the cloud plugin upload the current local database.
        income:close()
        logger.info("AutoStatsSync: no remote database yet; publishing local database")
        return true
    end
    if not databaseIsValid(income) then
        income:close()
        logger.err("AutoStatsSync: remote database failed integrity check")
        return false
    end
    income:close()

    local conn = SQ3.open(local_path)
    local local_version = databaseVersion(conn)
    if not local_version or local_version == 0 then
        logger.err("AutoStatsSync: local statistics database is invalid")
        conn:close()
        return false
    end
    if local_version ~= income_version then
        logger.err("AutoStatsSync: schema mismatch; local:", local_version, "remote:", income_version)
        conn:close()
        return false
    end
    if not databaseIsValid(conn) then
        logger.err("AutoStatsSync: local database failed integrity check")
        conn:close()
        return false
    end
    conn:close()
    if not backupLocalDatabase() then
        return false
    end
    conn = SQ3.open(local_path)

    local attach_sql = "ATTACH " .. sqlQuote(income_path) .. " AS income_db;"
    local merge_sql = [[
        BEGIN IMMEDIATE;

        -- For books known on both devices, retain monotonic counters and the
        -- metadata belonging to whichever copy was opened most recently.
        UPDATE book AS local_book
        SET notes = MAX(COALESCE(local_book.notes, 0), COALESCE(remote_book.notes, 0)),
            highlights = MAX(COALESCE(local_book.highlights, 0), COALESCE(remote_book.highlights, 0)),
            pages = CASE WHEN COALESCE(remote_book.last_open, 0) > COALESCE(local_book.last_open, 0)
                         THEN remote_book.pages ELSE local_book.pages END,
            series = CASE WHEN COALESCE(remote_book.last_open, 0) > COALESCE(local_book.last_open, 0)
                          THEN remote_book.series ELSE local_book.series END,
            language = CASE WHEN COALESCE(remote_book.last_open, 0) > COALESCE(local_book.last_open, 0)
                            THEN remote_book.language ELSE local_book.language END,
            last_open = MAX(COALESCE(local_book.last_open, 0), COALESCE(remote_book.last_open, 0))
        FROM income_db.book AS remote_book
        WHERE (local_book.title, local_book.authors, local_book.md5) =
              (remote_book.title, remote_book.authors, remote_book.md5);

        -- KOReader's own unique book identity is title + authors + partial MD5.
        INSERT INTO book (
            title, authors, notes, last_open, highlights, pages, series,
            language, md5, total_read_time, total_read_pages
        )
        SELECT title, authors, notes, last_open, highlights, pages, series,
               language, md5, total_read_time, total_read_pages
        FROM income_db.book AS remote_book
        WHERE NOT EXISTS (
            SELECT 1 FROM book AS local_book
            WHERE (local_book.title, local_book.authors, local_book.md5) =
                  (remote_book.title, remote_book.authors, remote_book.md5)
        );

        DROP TABLE IF EXISTS temp.autostatsync_book_map;
        CREATE TEMP TABLE autostatsync_book_map AS
        SELECT local_book.id AS local_id, remote_book.id AS remote_id
        FROM book AS local_book
        INNER JOIN income_db.book AS remote_book
        ON (local_book.title, local_book.authors, local_book.md5) =
           (remote_book.title, remote_book.authors, remote_book.md5);

        -- The UNIQUE constraint defines an event as book + page + start_time.
        -- Shared baseline rows are ignored. If the same event has two durations,
        -- retain the longer observation and its associated layout page count.
        INSERT INTO page_stat_data (id_book, page, start_time, duration, total_pages)
        SELECT book_map.local_id, remote_event.page, remote_event.start_time,
               remote_event.duration, remote_event.total_pages
        FROM income_db.page_stat_data AS remote_event
        INNER JOIN autostatsync_book_map AS book_map
        ON remote_event.id_book = book_map.remote_id
        ON CONFLICT(id_book, page, start_time) DO UPDATE SET
            total_pages = CASE
                WHEN excluded.duration > page_stat_data.duration THEN excluded.total_pages
                ELSE page_stat_data.total_pages
            END,
            duration = MAX(page_stat_data.duration, excluded.duration);

        -- Never add cached totals. Rebuild them from the union of raw events.
        UPDATE book SET
            total_read_pages = COALESCE((
                SELECT count(DISTINCT page) FROM page_stat WHERE id_book = book.id
            ), 0),
            total_read_time = COALESCE((
                SELECT sum(duration) FROM page_stat WHERE id_book = book.id
            ), 0);
    ]]

    local attached, attach_error = pcall(conn.exec, conn, attach_sql)
    if not attached then
        logger.err("AutoStatsSync: cannot attach remote database:", attach_error)
        conn:close()
        return false
    end

    local merged, merge_error = pcall(conn.exec, conn, merge_sql)
    if merged and databaseIsValid(conn) then
        local committed, commit_error = pcall(conn.exec, conn, "COMMIT;")
        if not committed then
            merged = false
            merge_error = commit_error
        end
    else
        merged = false
        merge_error = merge_error or "integrity check failed after merge"
    end

    if not merged then
        pcall(conn.exec, conn, "ROLLBACK;")
        logger.err("AutoStatsSync: merge failed:", merge_error)
    end
    pcall(conn.exec, conn, "DETACH income_db;")
    conn:close()
    return merged
end

function AutoStatsSync:saveSettings()
    G_reader_settings:saveSetting("autostatsync", self.settings)
    G_reader_settings:flush()
end

function AutoStatsSync:setStatus(status)
    self.settings.last_status = status
    self:saveSettings()
end

function AutoStatsSync:getStatisticsPlugin()
    return self.ui and self.ui.statistics
end

function AutoStatsSync:getServer()
    local statistics = self:getStatisticsPlugin()
    return statistics and statistics.settings and statistics.settings.sync_server
end

function AutoStatsSync:refreshCurrentBookCache()
    local statistics = self:getStatisticsPlugin()
    if not statistics or not statistics.id_curr_book or not statistics.getPageTimeTotalStats then
        return
    end
    local ok, pages, duration = pcall(
        statistics.getPageTimeTotalStats,
        statistics,
        statistics.id_curr_book
    )
    if ok then
        statistics.book_read_pages = tonumber(pages) or 0
        statistics.book_read_time = tonumber(duration) or 0
        if statistics.book_read_pages > 0 then
            statistics.avg_time = statistics.book_read_time / statistics.book_read_pages
        end
    else
        logger.warn("AutoStatsSync: could not refresh current-book statistics cache:", pages)
    end
end

function AutoStatsSync:installSafeManualMerge()
    local statistics = self:getStatisticsPlugin()
    if statistics and statistics.onSync ~= AutoStatsSync.mergeDatabases then
        self.original_statistics_merge = statistics.onSync
        statistics.onSync = AutoStatsSync.mergeDatabases
        self.overridden_statistics = statistics
        logger.info("AutoStatsSync: installed append-only merger for manual statistics sync")
    end
end

function AutoStatsSync:showError(message, interactive)
    logger.warn("AutoStatsSync:", message)
    self:setStatus(message)
    if interactive then
        UIManager:show(InfoMessage:new{
            text = message,
            timeout = 4,
        })
    end
end

function AutoStatsSync:finishAttempt(ok, message, interactive)
    self.sync_in_progress = false
    UIManager:unschedule(self.watchdog_task)
    if ok then
        self:setStatus(message or _("Successfully synchronized"))
        if interactive then
            UIManager:show(InfoMessage:new{
                text = message or _("Successfully synchronized."),
                timeout = 3,
            })
        end
    else
        local failure = message or _("Synchronization failed; local data is safe")
        self:setStatus(failure)
        Notification:notify(failure)
    end
end

function AutoStatsSync:performSync(provider, server, interactive)
    local income_path = DB_PATH .. ".autostatsync-temp"
    local cached_path = DB_PATH .. ".sync"
    local remote_folder = server.url:sub(-1) == "/" and server.url:sub(1, -2) or server.url
    local remote_path = remote_folder .. "/" .. ffiUtil.basename(DB_PATH)
    local response_code

    for attempt = 1, MAX_CONFLICT_RETRIES do
        os.remove(income_path)
        local etag
        response_code, etag = provider.downloadFile(remote_path, income_path)
        if response_code ~= 200 and response_code ~= 404 then
            os.remove(income_path)
            self:finishAttempt(false, _("WebDAV download failed; local data is safe"), interactive)
            return
        end

        local merged = AutoStatsSync.mergeDatabases(DB_PATH, cached_path, income_path)
        if not merged then
            os.remove(income_path)
            self:finishAttempt(false, _("Database merge failed; no data was uploaded"), interactive)
            return
        end
        self:refreshCurrentBookCache()

        response_code = provider.uploadFile(server.url, DB_PATH, etag, true)
        if response_code ~= 412 then
            break
        end
        logger.info("AutoStatsSync: remote changed during upload; retrying", attempt)
    end

    os.remove(income_path)
    if type(response_code) == "number" and response_code >= 200 and response_code < 300 then
        os.remove(cached_path)
        ffiUtil.copyFile(DB_PATH, cached_path)
        self:finishAttempt(true, _("Successfully synchronized"), interactive)
    elseif response_code == 412 then
        self:finishAttempt(false, _("WebDAV kept changing; synchronization will retry later"), interactive)
    else
        -- The local database already contains the downloaded union at this
        -- point. A later run can safely retry the upload without losing it.
        self:finishAttempt(false, _("Upload failed; merged data remains on this device"), interactive)
    end
end

function AutoStatsSync:requestSync(reason, interactive)
    if not self.settings.enabled and not interactive then
        return
    end
    if self.sync_in_progress then
        logger.info("AutoStatsSync: sync already in progress; skipping", reason)
        return
    end
    if lfs.attributes(DB_PATH, "mode") ~= "file" then
        self:showError(_("Reading statistics database does not exist yet."), interactive)
        return
    end

    local cloudstorage = self.ui and self.ui.cloudstorage
    local server = self:getServer()
    if not cloudstorage then
        self:showError(_("KOReader's Cloud storage plugin is not enabled."), interactive)
        return
    end
    if not server then
        self:showError(_("Configure Reading statistics → Cloud sync first."), interactive)
        return
    end
    if type(server.url) ~= "string" or server.url == "" then
        self:showError(_("The Reading statistics WebDAV folder is invalid."), interactive)
        return
    end
    if server.type ~= "webdav" then
        self:showError(_("Automatic statistics sync currently requires a WebDAV server."), interactive)
        return
    end
    if self.settings.only_when_online and not NetworkMgr:isOnline() then
        logger.info("AutoStatsSync: offline; keeping local changes for the next sync")
        self:setStatus(_("Offline; local changes will sync next time"))
        return
    end

    self.sync_in_progress = true
    self.settings.last_attempt = os.time()
    self:setStatus(_("Synchronizing…"))
    UIManager:scheduleIn(WATCHDOG_SECONDS, self.watchdog_task)

    local provider = cloudstorage.providers and cloudstorage.providers.webdav
    if not provider then
        self.sync_in_progress = false
        UIManager:unschedule(self.watchdog_task)
        self:showError(_("KOReader's WebDAV provider is unavailable."), interactive)
        return
    end
    provider.base = util.tableDeepCopy(server)

    logger.info("AutoStatsSync: starting", reason, "sync")
    provider.run(function()
        UIManager:nextTick(function()
            local ok, err = pcall(self.performSync, self, provider, server, interactive)
            if not ok then
                logger.err("AutoStatsSync: unexpected synchronization error:", err)
                self:finishAttempt(false, _("Unexpected sync error; local data is safe"), interactive)
            end
        end)
    end)
end

function AutoStatsSync:init()
    self.settings = G_reader_settings:readSetting("autostatsync", AutoStatsSync.default_settings)
    for key, value in pairs(AutoStatsSync.default_settings) do
        if self.settings[key] == nil then
            self.settings[key] = value
        end
    end
    self.watchdog_task = function()
        if self.sync_in_progress then
            self.sync_in_progress = false
            self:setStatus(_("Sync timed out; it will be retried later"))
        end
    end
    self.open_sync_task = function()
        self:installSafeManualMerge()
        self:requestSync("book-open", false)
    end
    self.ui.menu:registerToMainMenu(self)
end

function AutoStatsSync:onReaderReady()
    self:installSafeManualMerge()
    if self.settings.enabled and self.settings.sync_on_open then
        UIManager:scheduleIn(OPEN_SYNC_DELAY_SECONDS, self.open_sync_task)
    end
end

function AutoStatsSync:onCloseDocument()
    UIManager:unschedule(self.open_sync_task)
    if self.settings.enabled and self.settings.sync_on_close then
        -- Run on the next UI tick so statistics.koplugin has first flushed the
        -- final page duration to SQLite, regardless of plugin dispatch order.
        UIManager:nextTick(function()
            self:requestSync("book-close", false)
        end)
    end
end

function AutoStatsSync:onCloseWidget()
    UIManager:unschedule(self.open_sync_task)
    UIManager:unschedule(self.watchdog_task)
    if self.overridden_statistics
        and self.overridden_statistics.onSync == AutoStatsSync.mergeDatabases then
        self.overridden_statistics.onSync = self.original_statistics_merge
    end
end

function AutoStatsSync:addToMainMenu(menu_items)
    menu_items.autostatsync = {
        text = _("Automatic statistics sync"),
        sorting_hint = "tools",
        sub_item_table = {
            {
                text = _("Enable automatic sync"),
                checked_func = function() return self.settings.enabled end,
                callback = function()
                    self.settings.enabled = not self.settings.enabled
                    self:saveSettings()
                end,
            },
            {
                text = _("Sync after opening a book"),
                checked_func = function() return self.settings.sync_on_open end,
                enabled_func = function() return self.settings.enabled end,
                callback = function()
                    self.settings.sync_on_open = not self.settings.sync_on_open
                    self:saveSettings()
                end,
            },
            {
                text = _("Sync after closing a book"),
                checked_func = function() return self.settings.sync_on_close end,
                enabled_func = function() return self.settings.enabled end,
                callback = function()
                    self.settings.sync_on_close = not self.settings.sync_on_close
                    self:saveSettings()
                end,
            },
            {
                text = _("Only sync when Wi-Fi is already online"),
                checked_func = function() return self.settings.only_when_online end,
                callback = function()
                    self.settings.only_when_online = not self.settings.only_when_online
                    self:saveSettings()
                end,
                separator = true,
            },
            {
                text = _("Sync now"),
                callback = function()
                    self:requestSync("manual", true)
                end,
                keep_menu_open = true,
            },
            {
                text_func = function()
                    return _("Status: ") .. (self.settings.last_status or _("Unknown"))
                end,
                enabled = false,
            },
        },
    }
end

return AutoStatsSync
