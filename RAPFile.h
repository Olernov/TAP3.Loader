class RAPFile
{
public:
	RAPFile(otl_connect&, Config&);
	int CreateRAPFile(ReturnBatch* returnBatch, ReturnDetail* returnDetail[],
		int returnDetailsCount, long long totalSevereReturn, long roamingHubID,
		string tapSender, string tapRecipient, string tapAvailableStamp, string fileTypeIndicator,
		long& rapFileID, string& rapSequenceNum);
private:
	otl_connect& m_otlConnect;
	Config& m_config;
	
	int OctetString_fromInt64(OCTET_STRING& octetStr, long long value);
	bool UploadFileToFtp(string filename, string fullFileName, FtpSetting ftpSetting);
	int EncodeAndUpload(ReturnBatch* returnBatch, string filename, string roamingHubName);
};


