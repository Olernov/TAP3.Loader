#include "OTL_Header.h"
#include "TAP_Constants.h"
#include "DataInterchange.h"
#include "BatchControlInfo.h"
#include "ReturnBatch.h"
#include "ConfigContainer.h"
#include "RAPFile.h"
#include "CallValidator.h"
#include "TAPValidator.h"

using namespace std;

extern long long OctetStr2Int64(const OCTET_STRING_t& octetStr);
extern void log(string filename, short msgType, string msgText, string dbConnectString = "");

CallValidator::CallValidator(otl_connect& otlConnect, const TransferBatch* transferBatch, Config& config, long roamingHubID) :
	m_otlConnect(otlConnect),
	m_transferBatch(transferBatch),
	m_config(config),
	m_rapFile(otlConnect, config, roamingHubID)	
{}


CallValidationResult CallValidator::ValidateCall(long long eventID, CallTypeForValidation callType, 
	int callIndex, long iotValidationMode)
{
	CallAgeValidationResult callAgeValid = ValidateAgeAndCreateRAP(eventID, callType, callIndex);
	IOTValidationResult iotValidationResult;

	switch(callAgeValid) {
	case CALL_AGE_VALID:
		iotValidationResult = ValidateIOTAndCreateRAP(eventID, callType, callIndex, iotValidationMode);
		switch(iotValidationResult) {
		case IOT_VALID:
			return CALL_VALID;
		case IOT_VALIDATION_IMPOSSIBLE:
			return UNABLE_TO_VALIDATE_CALL;
		default:
			return CALL_INVALID;
		}
	case CALL_AGE_EXCEEDED:
		return CALL_INVALID;
	default:
		return UNABLE_TO_VALIDATE_CALL;
	}
}


CallAgeValidationResult CallValidator::ValidateAgeAndCreateRAP(long long eventID, CallTypeForValidation callType, 
	int callIndex)
{
	otl_nocommit_stream otlStream;
	otlStream.open(1, "call BILLING.TAP3.IsCallAgeValid(:event_id /*bigint,in*/, :calltype /*long,in*/) "
		"into :res /*long,out*/", m_otlConnect);
	otlStream << eventID << (long) callType;
	long res;
	otlStream >> res;
	otlStream.close();

	CallAgeValidationResult callAgeValid = static_cast<CallAgeValidationResult>(res);
	switch(callAgeValid) {
	case CALL_AGE_VALID:
		return callAgeValid;
	case CALL_AGE_EXCEEDED:
		if (!m_rapFile.IsInitialized()) {
			m_rapFile.Initialize(m_transferBatch);
		}
		m_rapFile.AddReturnDetail(CreateReturnDetailForCallAgeError(callIndex, CALL_OLDER_THAN_ALLOWED_BY_BARG), 
			CallTotalCharge(callIndex));
		otlStream.open(1, "call BILLING.TAP3.SetRAPFileSeqNumForEvent(:event_id /*bigint,in*/, :call_type /*long,in*/, "
			":rapseqnum /*char[10],in*/)", m_otlConnect);
		otlStream
			<< eventID
			<< static_cast<long>(callType)
			<< m_rapFile.GetSequenceNumber();
		otlStream.close();
		return CALL_AGE_EXCEEDED;
	default:
		log(m_rapFile.GetName(), LOG_ERROR, "Функция TAP3.IsCallAgeValid вернула недопустимый код: " + 
			to_string(static_cast<long long>(res)));
		return CALL_AGE_ERROR;
	}
}


IOTValidationResult CallValidator::ValidateIOTAndCreateRAP(long long eventID, CallTypeForValidation callType, int callIndex, long iotValidationMode)
{
	if (iotValidationMode == IOT_NO_NEED) {
		return IOT_VALID;
	}
	otl_nocommit_stream otlStream;
	switch (callType) {
	case TELEPHONY_CALL:
		otlStream.open(1, "call BILLING.TAP3_IOT.ValidateTapCall(:event_id /*bigint,in*/, :err_descr /*char[255],out*/,"
			":iot_date /*char[50],out*/, :exp_charge /*double,out*/, :calculation /*char[200],out*/) "
			"into :res /*long,out*/", m_otlConnect);
		break;
	case GPRS_CALL:
		otlStream.open(1, "call BILLING.TAP3_IOT.ValidateGPRSCall(:event_id /*bigint,in*/, :err_descr /*char[255],out*/,"
			":iot_date /*char[50],out*/, :exp_charge /*double,out*/, :calculation /*char[200],out*/) "
			"into :res /*long,out*/", m_otlConnect);
		break;
	default:
		return IOT_VALIDATION_IMPOSSIBLE;
	}
	otlStream << eventID;
	long validationRes;
	string errorDescr, iotDate, calculation;
	double expectedCharge;
	otlStream
		>> errorDescr
		>> iotDate
		>> expectedCharge
		>> calculation
		>> validationRes;
	otlStream.close();
	if (validationRes == VALIDATION_IMPOSSIBLE)
		return IOT_VALIDATION_IMPOSSIBLE;

	if (validationRes != RAEX_IOT_VALID) {
		if (iotValidationMode == IOT_RAP_DROPOUT_ALERT) {
			if (!m_rapFile.IsInitialized()) {
				if (!m_rapFile.Initialize(m_transferBatch)) {
					return IOT_VALIDATION_IMPOSSIBLE;
				}
			}
			m_rapFile.AddReturnDetail(
				CreateReturnDetailForIOTError(callIndex, CHARGE_NOT_IN_ROAMING_AGREEMENT, 
					iotDate, expectedCharge, calculation), CallTotalCharge(callIndex));
		}
	}

	otlStream.open(1, "call BILLING.TAP3_IOT.SetValidationResult(:call_type /*short,in*/, :event_id /*bigint,in*/, "
		":iot_mode /*long,in*/, :res /*long,in*/, :err_descr /*char[255],in*/,"
		":rapseqnum /*char[10],in*/)", m_otlConnect);
	otlStream
		<< static_cast<short>(callType)
		<< eventID
		<< iotValidationMode
		<< validationRes
		<< errorDescr
		<< m_rapFile.GetSequenceNumber();
	otlStream.close();
	return static_cast<IOTValidationResult>(validationRes);
}


ReturnDetail* CallValidator::CreateReturnDetailForIOTError(int callIndex, int errorCode, 
	string iotDate, double expectedCharge, string calculation)
{
	ReturnDetail* returnDetail = CreateReturnDetail(callIndex);
	ErrorDetail* errorDetail = CreateCommonErrorDetail(callIndex, errorCode);
	
	switch (m_transferBatch->callEventDetails->list.array[callIndex]->present) {
	case CallEventDetail_PR_mobileOriginatedCall:
	case CallEventDetail_PR_mobileTerminatedCall:
		AddErrorContext(errorDetail, 4, asn_DEF_BasicServiceUsedList.tags[0], 1 /*TODO: think about correct item occurrence*/);
		AddErrorContext(errorDetail, 5, asn_DEF_BasicServiceUsed.tags[0], 0);
		AddErrorContext(errorDetail, 6, asn_DEF_ChargeInformationList.tags[0], 1);
		AddErrorContext(errorDetail, 7, asn_DEF_ChargeInformation.tags[0], 0);
		AddErrorContext(errorDetail, 8, asn_DEF_ChargeDetailList.tags[0], 1);
		AddErrorContext(errorDetail, 9, asn_DEF_ChargeDetail.tags[0], 0);
		AddErrorContext(errorDetail, 10, asn_DEF_Charge.tags[0], 0);
		break;
	case CallEventDetail_PR_gprsCall:
		AddErrorContext(errorDetail, 4, asn_DEF_GprsServiceUsed.tags[0], 1);
		AddErrorContext(errorDetail, 5, asn_DEF_ChargeInformationList.tags[0], 1);
		AddErrorContext(errorDetail, 6, asn_DEF_ChargeInformation.tags[0], 0);
		AddErrorContext(errorDetail, 7, asn_DEF_ChargeDetailList.tags[0], 1);
		AddErrorContext(errorDetail, 8, asn_DEF_ChargeDetail.tags[0], 0);
		AddErrorContext(errorDetail, 9, asn_DEF_Charge.tags[0], 0);
		break;
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.severeReturn.errorDetail, errorDetail);
	if (!iotDate.empty()) {
		iotDate = "IOTDate:" + iotDate;
		returnDetail->choice.severeReturn.operatorSpecList = (OperatorSpecList*)calloc(1, sizeof(OperatorSpecList));
		OperatorSpecInformation_t* octetStrIotDate = OCTET_STRING_new_fromBuf(&asn_DEF_OperatorSpecInformation, 
			iotDate.c_str(), iotDate.size());
		ASN_SEQUENCE_ADD(returnDetail->choice.severeReturn.operatorSpecList, octetStrIotDate);
		
		char strExpectedCharge[100];
		snprintf(strExpectedCharge, 100, "ExpCharge:%ld", 
			(long) (expectedCharge * (double) pow((double) 10, *m_transferBatch->accountingInfo->tapDecimalPlaces)));
		OperatorSpecInformation_t* octetStrExpCharge = OCTET_STRING_new_fromBuf(&asn_DEF_OperatorSpecInformation, 
			strExpectedCharge, strlen(strExpectedCharge));
		ASN_SEQUENCE_ADD(returnDetail->choice.severeReturn.operatorSpecList, octetStrExpCharge);

		if(!calculation.empty()) {
			calculation = "Calculation:" + calculation;
			OperatorSpecInformation_t* octetStrCalc = OCTET_STRING_new_fromBuf(&asn_DEF_OperatorSpecInformation, 
				calculation.c_str(), calculation.size());
			ASN_SEQUENCE_ADD(returnDetail->choice.severeReturn.operatorSpecList, octetStrCalc);
		}
	}
	return returnDetail;
}


ReturnDetail* CallValidator::CreateReturnDetailForCallAgeError(int callIndex, int errorCode)
{
	ReturnDetail* returnDetail = CreateReturnDetail(callIndex);
	ErrorDetail* errorDetail = CreateCommonErrorDetail(callIndex, errorCode);
	ASN_SEQUENCE_ADD(&returnDetail->choice.severeReturn.errorDetail, errorDetail);
	return returnDetail;
}


ErrorDetail* CallValidator::CreateCommonErrorDetail(int callIndex, int errorCode)
{
	ErrorDetail* errorDetail = (ErrorDetail*)calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;
	errorDetail->errorContext = (ErrorContextList*)calloc(1, sizeof(ErrorContextList));

	AddErrorContext(errorDetail, 1, asn_DEF_TransferBatch.tags[0], 0);
	AddErrorContext(errorDetail, 2, asn_DEF_CallEventDetailList.tags[0], callIndex + 1);
	switch (m_transferBatch->callEventDetails->list.array[callIndex]->present) {
	case CallEventDetail_PR_mobileOriginatedCall:
		AddErrorContext(errorDetail, 3, asn_DEF_MobileOriginatedCall.tags[0], 1);
		break;
	case CallEventDetail_PR_mobileTerminatedCall:
		AddErrorContext(errorDetail, 3, asn_DEF_MobileTerminatedCall.tags[0], 1);
		break;
	case CallEventDetail_PR_gprsCall:
		AddErrorContext(errorDetail, 3, asn_DEF_GprsCall.tags[0], 1);
		break;
	}
	return errorDetail;
}


ReturnDetail* CallValidator::CreateReturnDetail(int callIndex)
{
	ReturnDetail* returnDetail = (ReturnDetail*)calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_severeReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.severeReturn.fileSequenceNumber,
		(const char*)m_transferBatch->batchControlInfo->fileSequenceNumber->buf,
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.severeReturn.callEventDetail = *m_transferBatch->callEventDetails->list.array[callIndex];
	return returnDetail;
}


void CallValidator::AddErrorContext(ErrorDetail* errorDetail, int ctxLevel, int pathItemId, int itemOccurrence)
{
	ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext->pathItemId = pathItemId >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext->itemLevel = ctxLevel;
	if (itemOccurrence > 0) {
		errorContext->itemOccurrence = (ItemOccurrence_t*)calloc(1, sizeof(ItemOccurrence_t));
		*errorContext->itemOccurrence = itemOccurrence;
	}
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
}


// Calculates call total charge multiplied by TAP power based on TAP decimal places
long CallValidator::CallTotalCharge(int callIndex)
{
	ChargeInformationList* chargeInfoList;
	const std::string CHARGED_ITEM_TOTAL_CHARGE = "00";
	switch (m_transferBatch->callEventDetails->list.array[callIndex]->present) {
	case CallEventDetail_PR_mobileOriginatedCall:
		chargeInfoList = m_transferBatch->callEventDetails->list.array[callIndex]->
			choice.mobileOriginatedCall.basicServiceUsedList->list.array[0]->chargeInformationList;
		break;
	case CallEventDetail_PR_mobileTerminatedCall:
		chargeInfoList = m_transferBatch->callEventDetails->list.array[callIndex]->
			choice.mobileTerminatedCall.basicServiceUsedList->list.array[0]->chargeInformationList;
		break;
	case CallEventDetail_PR_gprsCall:
		chargeInfoList = m_transferBatch->callEventDetails->list.array[callIndex]->
			choice.gprsCall.gprsServiceUsed->chargeInformationList;
	}
	long totalCharge = 0;
	for (int chr_index = 0; chr_index < chargeInfoList->list.count; chr_index++)
		for (int chr_det_index = 0; chr_det_index < chargeInfoList->list.array[chr_index]->chargeDetailList->list.count; chr_det_index++) {
			if (string((char*) chargeInfoList->list.array[chr_index]->chargeDetailList->list.array[chr_det_index]->
					chargeType->buf) == CHARGED_ITEM_TOTAL_CHARGE)
				totalCharge += OctetStr2Int64(*chargeInfoList->list.array[chr_index]->chargeDetailList->
					list.array[chr_det_index]->charge);
	}
	return totalCharge;
}


RAPFile& CallValidator::GetRAPFile()
{
	return m_rapFile;
}
