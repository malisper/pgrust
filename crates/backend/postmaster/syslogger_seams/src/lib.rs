seam_core::seam!(
    pub fn write_syslogger_file(data: &[u8], dest: i32)
);

seam_core::seam!(
    // RemoveLogrotateSignalFiles (syslogger.c).
    pub fn remove_logrotate_signal_files()
);

seam_core::seam!(
    // CheckLogrotateSignal (syslogger.c).
    pub fn check_logrotate_signal() -> bool
);

seam_core::seam!(
    // SysLoggerMain (syslogger.c): launch_backend's child table entry; the
    // seam breaks the syslogger -> launch_backend -> syslogger cycle.
    pub fn sys_logger_main(startup_data: &types_startup::StartupData) -> !
);
