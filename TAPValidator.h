// See class description at TAPValidator.cpp

enum TAPValidationResult
{
	TAP_VALID = 0,
	FATAL_ERROR = 1,
	SEVERE_ERROR = 2,
	VALIDATION_IMPOSSIBLE = 3,
	WRONG_ADDRESSEE = 4
};

enum TAPValidationErrors
{
	TF_BATCH_AUDIT_CONTROL_INFO_MISSING = 30
};

class TAPValidator
{
public:
	TAPValidator(otl_connect&, Config&);
	TAPValidationResult Validate(DataInterChange*);
private:
	otl_connect& m_otlConnect;
	Config& m_config;

	ReturnBatch* FillReturnBatch(const TransferBatch& transferBatch, ReturnDetail* returnDetail);
	TAPValidationResult ValidateTransferBatch(const TransferBatch&);
	TAPValidationResult ValidateNotification(const Notification&);
	int OctetString_fromInt64(OCTET_STRING& octetStr, long long value);
	bool UploadFileToFtp(string filename, string fullFileName, FtpSetting ftpSetting, string roamingHubName);
	int EncodeAndUpload(string filename, string fileTypeDescr, asn_TYPE_descriptor_t* pASNTypeDescr, 
		ReturnBatch* pASNStructure, string roamingHubName);
};