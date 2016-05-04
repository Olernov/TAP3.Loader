class RAPFile
{
public:
	RAPFile(otl_connect&, Config&);
	int CreateRAPFile(ReturnBatch* returnBatch, ReturnDetail* returnDetail, long roamingHubID, string sender, string recipient,
		string tapAvailableStamp, string fileTypeIndicator, long& rapFileID, string& rapFilename);
private:
	otl_connect& m_otlConnect;
	Config& m_config;
	
	int OctetString_fromInt64(OCTET_STRING& octetStr, long long value);
	bool UploadFileToFtp(string filename, string fullFileName, FtpSetting ftpSetting);
	int EncodeAndUpload(ReturnBatch* returnBatch, string filename, string roamingHubName);
};


