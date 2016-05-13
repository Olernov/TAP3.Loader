#include "OTL_Header.h"
#include "TAP_Constants.h"
#include "DataInterchange.h"
#include "BatchControlInfo.h"
#include "ReturnBatch.h"
#include "ConfigContainer.h"
#include "CallValidator.h"
#include "TAPValidator.h"
#include "RAPFile.h"

using namespace std;

#define NO_ASN_ITEMS	vector<ErrContextAsnItem>()

extern void log(string filename, short msgType, string msgText);
extern void log(short msgType, string msgText);
extern long long OctetStr2Int64(const OCTET_STRING_t& octetStr);
extern int LoadReturnBatchToDB(ReturnBatch* returnBatch, long fileID, long roamingHubID, string rapFilename, long fileStatus);
extern int write_out(const void *buffer, size_t size, void *app_key);
extern "C" int ncftp_main(int argc, char **argv, char* result);

CallValidator::CallValidator(otl_connect& otlConnect, const TransferBatch* transferBatch, Config& config) :
	m_otlConnect(otlConnect),
	m_transferBatch(transferBatch),
	m_config(config),
	m_returnBatch(NULL),
	m_totalSevereReturn(0)
{}


void CallValidator::CreateReturnBatch()
{
	m_returnBatch = (ReturnBatch*) calloc(1, sizeof(ReturnBatch));
}

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


void CallValidator::AddCallToReturnBatch(int callIndex, int errorCode)
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
	m_returnDetails.push_back(returnDetail);

	m_totalSevereReturn += CallTotalCharge(callIndex);
}


long CallValidator::ValidateCallIOT(long long eventID, CallTypeForValidation callType, int callIndex, long iotValidationMode)
{
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
	long res;
	string errorDescr, iotDate, calculation;
	double expectedCharge;
	otlStream
		>> errorDescr
		>> iotDate
		>> expectedCharge
		>> calculation
		>> res;

	otlStream.close();
	if (res != RAEX_IOT_VALID) {
		otlStream.open( 1,
			"insert into BILLING.TAP3_VALIDATION_LOG (EVENT_ID, VALIDATION_TIME, RESULT, DESCRIPTION) "
			"values ("
			  ":eventid /*bigint,in*/, sysdate, :res /*long,in*/, :descr/*char[255],in*/) ", m_otlConnect);
		otlStream
			<< eventID
			<< res
			<< errorDescr;
		otlStream.close();
		if(iotValidationMode >= IOT_RAP_DROPOUT_ALERT) {
			// TODO: update TAP3_Call set cancel_rap_seq_num = ...
			if (!m_returnBatch)
				CreateReturnBatch();
			AddCallToReturnBatch(callIndex, CHARGE_NOT_IN_ROAMING_AGREEMENT);
		}
	}
	return res;
}


bool CallValidator::GotReturnBatch()
{
	return (m_returnBatch != NULL);
}


int CallValidator::WriteRAPFile(long roamingHubID)
{
	RAPFile rapFile(m_otlConnect, m_config);
	
	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->recipient);
	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);
	assert(m_returnBatch);

	long rapFileID;
	string rapSequenceNum;
	int loadRes = rapFile.CreateRAPFile(
		m_returnBatch, 
		&m_returnDetails[0], 
		m_returnDetails.size(), 
		m_totalSevereReturn,
		roamingHubID, 
		(char*)m_transferBatch->batchControlInfo->sender->buf,
		(char*) m_transferBatch->batchControlInfo->recipient->buf, 
		(char*) m_transferBatch->batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf,
		(m_transferBatch->batchControlInfo->fileTypeIndicator ? 
			(char*) m_transferBatch->batchControlInfo->fileTypeIndicator->buf : ""),
		rapFileID, 
		rapSequenceNum);
	return loadRes;
}