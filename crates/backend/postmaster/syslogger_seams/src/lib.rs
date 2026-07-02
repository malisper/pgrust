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
