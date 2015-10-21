// See class description at TAPValidator.cpp

enum TAPValidationResult
{
	TAP_VALID = 0,
	FATAL_ERROR = 1,
	SEVERE_ERROR = 2,
	VALIDATION_IMPOSSIBLE = 3,
	WRONG_ADDRESSEE = 5
};

enum TAPValidationErrors
{
	TF_BATCH_BATCH_CONTROL_INFO_MISSING = 30,
	TF_BATCH_ACCOUNTING_INFO_MISSING = 31,
	TF_BATCH_NETWORK_INFO_MISSING = 32,
	TF_BATCH_CALL_EVENTS_MISSING = 35,
	TF_BATCH_AUDIT_CONTROL_INFO_MISSING = 36,

	BATCH_CTRL_FILE_AVAIL_TIMESTAMP_MISSING = 33,
	BATCH_CTRL_SPEC_VERSION_MISSING = 34,
	BATCH_CTRL_TRANSFER_CUTOFF_MISSING = 36,

	ACCOUNTING_LOCAL_CURRENCY_MISSING = 32,
	ACCOUNTING_CURRENCY_CONVERSION_MISSING = 34,
	ACCOUNTING_TAP_DECIMAL_PLACES_MISSING = 35
};

class TAPValidator
{
public:
	TAPValidator(otl_connect&, Config&, string, string);
	TAPValidationResult Validate(DataInterChange*);
private:
	otl_connect& m_otlConnect;
	Config& m_config;
	string m_rapFilename;
	string m_roamingHubName;

	ReturnBatch* FillReturnBatch(const TransferBatch& transferBatch, ReturnDetail* returnDetail);
	TAPValidationResult ValidateTransferBatch(const TransferBatch&);
	TAPValidationResult ValidateNotification(const Notification&);
	int OctetString_fromInt64(OCTET_STRING& octetStr, long long value);
	int GetASNStructSize(asn_TYPE_descriptor_t *td, void *sptr);
	//ReturnDetail* CreateTransferBatchError(const TransferBatch& transferBatch, int errorCode);
	int CreateTransferBatchRAPFile(string logMessage, const TransferBatch& transferBatch, int errorCode);
	ReturnDetail* CreateBatchControlInfoError(const TransferBatch& transferBatch, int errorCode);
	int CreateBatchControlInfoRAPFile(string logMessage, const TransferBatch& transferBatch, int errorCode);
	bool UploadFileToFtp(string filename, string fullFileName, FtpSetting ftpSetting);
	int EncodeAndUpload(string filename, string fileTypeDescr, asn_TYPE_descriptor_t* pASNTypeDescr, ReturnBatch* pASNStructure);
};