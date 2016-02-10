// See class description at TAPValidator.cpp

enum TAPConstants
{
	START_TAP_SEQUENCE_NUM = 1,
	END_TAP_SEQUENCE_NUM = 99999,
	TAP_DECIMAL_VALID_FROM = 0,
	TAP_DECIMAL_VALID_TO = 6,
	TAX_RATE_VALID_FROM = 0L,
	TAX_RATE_VALID_TO = 9999999L
};

enum TAPValidationResult
{
	TAP_VALID = 0,
	FATAL_ERROR = 1,
	SEVERE_ERROR = 2,
	VALIDATION_IMPOSSIBLE = 3,
	WRONG_ADDRESSEE = 5,
	FILE_DUPLICATION = 6
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

	FILE_SEQ_NUM_SYNTAX_ERROR = 10,
	FILE_SEQ_NUM_OUT_OF_RANGE = 20,
	FILE_SEQ_NUM_DUPLICATION = 201,

	ACCOUNTING_TAXATION_MISSING = 30,
	ACCOUNTING_DISCOUNTING_MISSING = 31,
	ACCOUNTING_LOCAL_CURRENCY_MISSING = 32,
	ACCOUNTING_CURRENCY_CONVERSION_MISSING = 34,
	ACCOUNTING_TAP_DECIMAL_PLACES_MISSING = 35,
	
	CURRENCY_CONVERSION_EXRATE_CODE_MISSING = 30,
	CURRENCY_CONVERSION_NUM_OF_DEC_PLACES_MISSING = 31,
	CURRENCY_CONVERSION_EXCHANGE_RATE_MISSING = 33,
	CURRENCY_CONVERSION_EXRATE_CODE_DUPLICATION =34,
	TAP_DECIMAL_OUT_OF_RANGE = 20,
	TAXATION_TAXRATE_CODE_MISSING = 30,
	TAXATION_TAX_TYPE_MISSING = 31,
	TAXATION_TAX_CODE_DUPLICATION =33,
	TAX_RATE_SYNTAX_ERROR = 10,
	TAX_RATE_OUT_OF_RANGE = 20,

	NETWORK_UTC_TIMEOFFSET_MISSING = 30,
	NETWORK_REC_ENTITY_MISSING = 33,

	REC_ENTITY_CODE_MISSING = 30,
	REC_ENTITY_TYPE_MISSING = 31,
	REC_ENTITY_IDENTIFICATION_MISSING = 32,
	REC_ENTITY_CODE_DUPLICATION = 33,

	AUDIT_CTRL_TOTAL_CHARGE_MISSING = 30,
	AUDIT_CTRL_TOTAL_TAX_VALUE_MISSING = 31,
	AUDIT_CTRL_TOTAL_DISCOUNT_MISSING = 32,
	AUDIT_CTRL_CALL_COUNT_MISSING = 33,
	
	CALL_COUNT_MISMATCH = 100,
	TOTAL_CHARGE_MISMATCH = 100
};

enum FileDuplicationCheckRes
{
	DUPLICATION_NOTFOUND	= 0,
	COPY_FOUND				= 1,
	DUPLICATION_FOUND		= 2,
	DUPLICATION_CHECK_ERROR = -1
};

class ErrContextAsnItem
{
public:
	ErrContextAsnItem(asn_TYPE_descriptor_t* asnType, long itemOccurence) : m_asnType(asnType), m_itemOccurence(itemOccurence) {}
	asn_TYPE_descriptor_t* m_asnType;
	long m_itemOccurence;
};

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


class TAPValidator
{
public:
	TAPValidator(otl_connect&, Config&);
	TAPValidationResult Validate(DataInterChange*, long);
	long GetRapFileID();
	string GetRapSequenceNum();
private:
	otl_connect& m_otlConnect;
	Config& m_config;
	
	TransferBatch* m_transferBatch;
	Notification* m_notification;

	long m_rapFileID;
	long m_roamingHubID;
	string m_rapSequenceNum;

	bool IsRecipientCorrect(string recipient);
	FileDuplicationCheckRes IsFileDuplicated();
	TAPValidationResult FileSequenceNumberControl();
	bool BatchContainsTaxes();
	bool BatchContainsDiscounts();
	bool ChargeInfoContainsPositiveCharges(ChargeInformation* chargeInfo);
	bool BatchContainsPositiveCharges();
	long long ChargeInfoListTotalCharge(ChargeInformationList* chargeInfoList);
	long long BatchTotalCharge();
	
	int CreateTransferBatchRAPFile(string logMessage, int errorCode);
	int CreateNotificationRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems);
	int CreateBatchControlInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems);
	int CreateAccountingInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems);
	int CreateNetworkInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems);
	int CreateAuditControlInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems);

	TAPValidationResult ValidateBatchControlInfo();
	TAPValidationResult ValidateNetworkInfo();
	TAPValidationResult ValidateAccountingInfo();
	TAPValidationResult ValidateAuditControlInfo();

	ReturnBatch* FillReturnBatch(ReturnDetail* returnDetail);
	TAPValidationResult ValidateTransferBatch();
	TAPValidationResult ValidateNotification();
};