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

	ACCOUNTING_TAXATION_MISSING = 30,
	ACCOUNTING_DISCOUNTING_MISSING = 31,
	ACCOUNTING_LOCAL_CURRENCY_MISSING = 32,
	ACCOUNTING_CURRENCY_CONVERSION_MISSING = 34,
	ACCOUNTING_TAP_DECIMAL_PLACES_MISSING = 35,
	
	NETWORK_UTC_TIMEOFFSET_MISSING = 30,
	NETWORK_REC_ENTITY_MISSING = 33
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
	
	TransferBatch* m_transferBatch;

	int OctetString_fromInt64(OCTET_STRING& octetStr, long long value);
	bool BatchContainsTaxes();
	bool BatchContainsDiscounts();
	bool ChargeInfoContainsPositiveCharges(ChargeInformation* chargeInfo);
	bool BatchContainsPositiveCharges();

	int CreateTransferBatchRAPFile(string logMessage, int errorCode);
	int CreateBatchControlInfoRAPFile(string logMessage, int errorCode);
	int CreateAccountingInfoRAPFile(string logMessage, int errorCode);
	int CreateNetworkInfoRAPFile(string logMessage, int errorCode);

	bool UploadFileToFtp(string filename, string fullFileName, FtpSetting ftpSetting);
	int EncodeAndUpload(string filename, string fileTypeDescr, asn_TYPE_descriptor_t* pASNTypeDescr, ReturnBatch* pASNStructure);

	ReturnBatch* FillReturnBatch(ReturnDetail* returnDetail);
	TAPValidationResult ValidateTransferBatch();
	TAPValidationResult ValidateNotification();
};