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

CallValidator::CallValidator(otl_connect& otlConnect, const TransferBatch* transferBatch, Config& config, long roamingHubID) :
	m_otlConnect(otlConnect),
	m_transferBatch(transferBatch),
	m_config(config),
	m_rapFile(otlConnect, config, roamingHubID),
	m_rapFileID(0)
{}


// Calculates call total charge multiplied by TAP power based on TAP decimal places
long CallValidator::CallTotalCharge(int callIndex)
{
	ChargeInformationList* chargeInfoList;
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
					chargeType->buf) == "00")
				totalCharge += OctetStr2Int64(*chargeInfoList->list.array[chr_index]->chargeDetailList->
					list.array[chr_det_index]->charge);
	}
	return totalCharge;
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


ReturnDetail* CallValidator::CreateReturnDetail(int callIndex, int errorCode)
{
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_severeReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.severeReturn.fileSequenceNumber, 
		(const char*) m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.severeReturn.callEventDetail = *m_transferBatch->callEventDetails->list.array[callIndex];
	
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));

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
		
	switch (m_transferBatch->callEventDetails->list.array[callIndex]->present) {
	case CallEventDetail_PR_mobileOriginatedCall:
	case CallEventDetail_PR_mobileTerminatedCall:
		AddErrorContext(errorDetail, 4, asn_DEF_BasicServiceUsedList.tags[0], 1 /*TODO: think about correct item occurrence*/);
		AddErrorContext(errorDetail, 5, asn_DEF_BasicServiceUsed.tags[0], 0);
		AddErrorContext(errorDetail, 6, asn_DEF_ChargeInformationList.tags[0], 1 /*TODO: think about correct item occurrence*/);
		AddErrorContext(errorDetail, 7, asn_DEF_ChargeInformation.tags[0], 0);
		AddErrorContext(errorDetail, 8, asn_DEF_ChargeDetailList.tags[0], 1 /*TODO: think about correct item occurrence*/);
		AddErrorContext(errorDetail, 9, asn_DEF_ChargeDetail.tags[0], 0);
		AddErrorContext(errorDetail, 10, asn_DEF_Charge.tags[0], 0);
		break;
	case CallEventDetail_PR_gprsCall:
		AddErrorContext(errorDetail, 4, asn_DEF_GprsServiceUsed.tags[0], 1);
		AddErrorContext(errorDetail, 5, asn_DEF_ChargeInformationList.tags[0], 1 /*TODO: think about correct item occurrence*/);
		AddErrorContext(errorDetail, 6, asn_DEF_ChargeInformation.tags[0], 0);
		AddErrorContext(errorDetail, 7, asn_DEF_ChargeDetailList.tags[0], 1 /*TODO: think about correct item occurrence*/);
		AddErrorContext(errorDetail, 8, asn_DEF_ChargeDetail.tags[0], 0);
		AddErrorContext(errorDetail, 9, asn_DEF_Charge.tags[0], 0);
		break;
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.severeReturn.errorDetail, errorDetail);
	return returnDetail;
}


long CallValidator::ValidateCallIOT(long long eventID, CallTypeForValidation callType, int callIndex, long iotValidationMode)
{
	if (iotValidationMode == IOT_NO_NEED)
		return TL_OK;
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
		return VALIDATION_IMPOSSIBLE;
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
		return VALIDATION_IMPOSSIBLE;

	if (validationRes != RAEX_IOT_VALID) {
		if (iotValidationMode == IOT_RAP_DROPOUT_ALERT) {
			if (!m_rapFile.Created()) {
				m_rapFile.Initialize((char*)m_transferBatch->batchControlInfo->sender->buf,
					(char*)m_transferBatch->batchControlInfo->recipient->buf,
					(char*)m_transferBatch->batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf,
					(m_transferBatch->batchControlInfo->fileTypeIndicator ?
					(char*)m_transferBatch->batchControlInfo->fileTypeIndicator->buf : ""));
			}
			m_rapFile.AddReturnDetail(CreateReturnDetail(callIndex, CHARGE_NOT_IN_ROAMING_AGREEMENT),
				CallTotalCharge(callIndex));
		}
	}

	otlStream.open(1, "call BILLING.TAP3_IOT.SetValidationResult(:call_type /*short,in*/, :event_id /*bigint,in*/, "
		":iot_mode /*long,in*/, :res /*long,in*/, :err_descr /*char[255],in*/,"
		":rapfileid /*long,in*/)", m_otlConnect);
	otlStream
		<< (short) callType
		<< eventID
		<< iotValidationMode
		<< validationRes
		<< errorDescr
		<< m_rapFileID;
	otlStream.close();
	return TL_OK;
}


RAPFile& CallValidator::GetRAPFile()
{
	return m_rapFile;
}
