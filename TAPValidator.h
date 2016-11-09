#pragma once
#include "RAPFile.h"

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
	FILE_DUPLICATION = 6,
	RAEX_IOT_VALID = 0,
	RAEX_IOT_INVALID = 7
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
	ACCOUNTING_EXCHARGE_RATE_LOWER = 200,
	ACCOUNTING_EXCHARGE_RATE_HIGHER = 201,
	
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

enum IncomingTAPAllowed
{
	INCOMING_TAP_ALLOWED = 0,
	INCOMING_TAP_NOT_ALLOWED = 1,
	UNABLE_TO_DETERMINE = 2
};

enum ExRateValidationRes
{
	EXRATE_VALID					= 0,
	EXRATE_WRONG_CODE				= -100,
	EXRATE_CURRENCY_NOT_FOUND		= -200,
	EXRATE_CURRENCY_MISMATCH		= -205,
	EXRATE_NOT_SET					= -207,
	EXRATE_HIGHER					= -210,
	EXRATE_LOWER					= -220
};


struct ErrContextAsnItem
{
	ErrContextAsnItem(asn_TYPE_descriptor_t* asnType, long itemOccurence) : m_asnType(asnType), m_itemOccurence(itemOccurence) {}
	asn_TYPE_descriptor_t* m_asnType;
	long m_itemOccurence;
};

struct ExchangeRate 
{
	ExchangeRateCode_t code;
	double rate;

	ExchangeRate(ExchangeRateCode_t code, double rate) :
		code(code),
		rate(rate)
	{}
};

class TAPValidator
{
public:
	TAPValidator(otl_connect& dbConnect, Config& config, long roamingHubID);
	void Validate(DataInterChange* dataInterchange);

	long GetRapFileID() const;
	string GetRapSequenceNum() const;
	long GetSenderNetworkID() const;
	long GetIOTValidationMode() const;
	TAPValidationResult GetValidationResult() const;
	const std::string& GetValidationError() const;
private:
	otl_connect& m_otlConnect;
	Config& m_config;
	
	TransferBatch* m_transferBatch;
	Notification* m_notification;

	//long m_rapFileID;
	long m_mobileNetworkID;
	long m_iotValidationMode;
	long m_roamingHubID;
	std::string m_rapSequenceNum;
	TAPValidationResult m_validationResult;
	std::string m_validationError;
	RAPFile m_rapFile;

	bool SetSenderNetworkID();
	void SetIOTValidationMode();
	bool IsRecipientCorrect(string recipient);
	bool IsTestFile();
	void SetErrorAndLog(std::string& error);
	FileDuplicationCheckRes IsFileDuplicated();
	IncomingTAPAllowed IsIncomingTAPAllowed();
	TAPValidationResult FileSequenceNumberControl();
	bool BatchContainsTaxes();
	bool BatchContainsDiscounts();
	bool ChargeInfoContainsPositiveCharges(ChargeInformation* chargeInfo);
	bool BatchContainsPositiveCharges();
	long long ChargeInfoListTotalCharge(ChargeInformationList* chargeInfoList);
	long long BatchTotalCharge();
	ExRateValidationRes ValidateChrInfoExRates(ChargeInformationList* pChargeInfoList, LocalTimeStamp_t* pCallTimestamp,
		const map<ExchangeRateCode_t, double>& exchangeRates, string tapLocalCurrency);
	ExRateValidationRes ValidateExchangeRates(const map<ExchangeRateCode_t, double>& exchangeRates, string tapLocalCurrency);
	
	int CreateTransferBatchRAPFile(string logMessage, int errorCode);
	int CreateNotificationRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems);
	int CreateBatchControlInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems);
	int CreateAccountingInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems);
	int CreateNetworkInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems);
	int CreateAuditControlInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems);
	int CreateAndUploadRapFile(ReturnDetail* returnDetail);

	int CreateSevereRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems);

	TAPValidationResult ValidateBatchControlInfo();
	TAPValidationResult ValidateNetworkInfo();
	TAPValidationResult ValidateAccountingInfo();
	TAPValidationResult ValidateAuditControlInfo();

	ReturnBatch* FillReturnBatch(ReturnDetail* returnDetail);
	TAPValidationResult ValidateTransferBatch();
	TAPValidationResult ValidateNotification();
};
