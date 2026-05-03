#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmlBinaryFormat {
    Base64,
    Hex,
}

impl Default for XmlBinaryFormat {
    fn default() -> Self {
        Self::Base64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmlOptionSetting {
    Document,
    Content,
}

impl Default for XmlOptionSetting {
    fn default() -> Self {
        Self::Content
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XmlConfig {
    pub option: XmlOptionSetting,
    pub binary: XmlBinaryFormat,
}

impl Default for XmlConfig {
    fn default() -> Self {
        Self {
            option: XmlOptionSetting::Content,
            binary: XmlBinaryFormat::Base64,
        }
    }
}

pub fn parse_xmlbinary(value: &str) -> Option<XmlBinaryFormat> {
    match value.trim().to_ascii_lowercase().as_str() {
        "base64" => Some(XmlBinaryFormat::Base64),
        "hex" => Some(XmlBinaryFormat::Hex),
        _ => None,
    }
}

pub fn format_xmlbinary(value: XmlBinaryFormat) -> &'static str {
    match value {
        XmlBinaryFormat::Base64 => "base64",
        XmlBinaryFormat::Hex => "hex",
    }
}

pub fn parse_xmloption(value: &str) -> Option<XmlOptionSetting> {
    match value.trim().to_ascii_lowercase().as_str() {
        "document" => Some(XmlOptionSetting::Document),
        "content" => Some(XmlOptionSetting::Content),
        _ => None,
    }
}

pub fn format_xmloption(value: XmlOptionSetting) -> &'static str {
    match value {
        XmlOptionSetting::Document => "DOCUMENT",
        XmlOptionSetting::Content => "CONTENT",
    }
}
