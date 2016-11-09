#pragma once

class RAPFile
{
public:
	RAPFile(otl_connect& otlConnect, Config& config, long roamingHubID);
	bool Initialize(const TransferBatch*);
	bool Initialize(const Notification*);
	void AddReturnDetail(ReturnDetail* returnDetail, long long callTotalCharge);
	void Finalize();
	int LoadToDB();
	int EncodeAndUpload();
	bool IsInitialized();
	std::string GetName() const;
	long GetID() const;
	std::string GetSequenceNumber() const;
private:
	otl_connect& m_otlConnect;
	Config& m_config;
	long m_roamingHubID;
	string m_roamingHubName;
	long m_fileID;
	string m_filename;
	string m_fileSeqNum;

	ReturnBatch* m_returnBatch;
	long long m_totalSevereReturn;
	int m_returnDetailsCount;
	
	bool Initialize(string tapSender, string tapRecipient, string tapAvailableStamp, string fileTypeIndicator);
	int OctetString_fromInt64(OCTET_STRING& octetStr, long long value);
	bool UploadFileToFtp(string filename, string fullFileName, FtpSetting ftpSetting);	
};

class RAPFileException : public std::runtime_error
{
public:
	explicit RAPFileException(const string& what_arg) :
		std::runtime_error(what_arg)
	{}
	
};
