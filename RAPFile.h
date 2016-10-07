#pragma once

class RAPFile
{
public:
	RAPFile(otl_connect& otlConnect, Config& config, long roamingHubID);
	int Initialize(const TransferBatch*);
	int Initialize(const Notification*);
	void AddReturnDetail(ReturnDetail* returnDetail, long long callTotalCharge);
	void Finalize();
	int LoadToDB();
	int EncodeAndUpload();
	bool IsInitialized();
	std::string GetName();
	long GetID();
	std::string GetSequenceNumber();
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
	
	int Initialize(string tapSender, string tapRecipient, string tapAvailableStamp, string fileTypeIndicator);
	int OctetString_fromInt64(OCTET_STRING& octetStr, long long value);
	bool UploadFileToFtp(string filename, string fullFileName, FtpSetting ftpSetting);	
};


