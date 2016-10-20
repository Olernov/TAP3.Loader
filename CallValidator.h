#pragma once
#include "RAPFile.h"

enum CallValidationErrors
{
	CHARGE_NOT_IN_ROAMING_AGREEMENT = 200,
	CALL_OLDER_THAN_ALLOWED_BY_BARG = 261
};


enum CallTypeForValidation
{
	TELEPHONY_CALL = 0,
	GPRS_CALL = 1
};


enum IOTValidationMode
{
	IOT_NO_NEED						= 968368583,
	IOT_ALERT						= 968368901,
	IOT_DROPOUT_ALERT				= 968369026,
	IOT_RAP_DROPOUT_ALERT			= 968369636
};


enum IOTValidationResult
{
	IOT_VALID                       = 0,
    RAEX_IOT_NOT_FOUND              = -101,
    ERROR_IN_RAEX_IOT               = -102,
    NO_APP_CHARGED_ITEM             = -105,
    PARTY_NUMBER_ANALYZE_ERROR      = -107,
    CTL_SET_NOT_FOUND               = -108,
    IOT_HIGHER_THAN_EXPECTED        = -120,
    IOT_LOWER_THAN_EXPECTED         = -130,
    NOT_IMPLEMENTED_YET             = -140,
    IOT_VALIDATION_IMPOSSIBLE       = -150
};

enum CallAgeValidationResult
{
	CALL_AGE_VALID			= 0,
	CALL_AGE_EXCEEDED		= -1,
	CALL_AGE_ERROR			= -10
};


enum CallValidationResult
{
	CALL_VALID,
	CALL_INVALID,
	UNABLE_TO_VALIDATE_CALL
};


class CallValidator
{
public:
	CallValidator(otl_connect& otlConnect, const TransferBatch* transferBatch, Config& config, long roamingHubID);
	CallValidationResult ValidateCall(long long eventID, CallTypeForValidation callType, int callIndex, long iotValidationMode);
	RAPFile& GetRAPFile();
private:
	otl_connect& m_otlConnect;
	Config& m_config;
	const TransferBatch* m_transferBatch;
	RAPFile m_rapFile;
	vector<ReturnDetail*> m_returnDetails;
	
	CallAgeValidationResult ValidateAgeAndCreateRAP(long long eventID, CallTypeForValidation callType,
		int callIndex);
	IOTValidationResult ValidateIOTAndCreateRAP(long long eventID, CallTypeForValidation callType, 
		int callIndex, long iotValidationMode);
	long CallTotalCharge(int callIndex);
	ReturnDetail* CreateReturnDetailForIOTError(int callIndex, int errorCode, 
		string iotDate, double expectedCharge, string calculation);
	ReturnDetail* CreateReturnDetailForCallAgeError(int callIndex, int errorCode);
	ReturnDetail* CreateReturnDetail(int callIndex);
	ErrorDetail* CreateCommonErrorDetail(int callIndex, int errorCode);
	void AddErrorContext(ErrorDetail* errorDetail, int ctxLevel, int pathItemId, int itemOccurrence);
};