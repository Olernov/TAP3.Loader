#include "OTL_Header.h"
#include "TAP_Constants.h"
#include "DataInterchange.h"
#include "BatchControlInfo.h"
#include "ReturnBatch.h"
#include "ConfigContainer.h"
#include "CallValidator.h"
#include "TAPValidator.h"

using namespace std;

#define NO_ASN_ITEMS	vector<ErrContextAsnItem>()

extern void log(string filename, short msgType, string msgText);
extern void log(short msgType, string msgText);
extern long long OctetStr2Int64(const OCTET_STRING_t& octetStr);
extern int LoadReturnBatchToDB(ReturnBatch* returnBatch, long fileID, long roamingHubID, string rapFilename, long fileStatus);
extern int write_out(const void *buffer, size_t size, void *app_key);
extern "C" int ncftp_main(int argc, char **argv, char* result);

CallValidator::CallValidator(otl_connect& otlConnect, const TransferBatch& transferBatch, Config& config) :
	m_otlConnect(otlConnect),
	m_transferBatch(transferBatch),
	m_config(config)
{}


//int CallValidator::CreateSevereRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems)
//{
//	log(LOG_ERROR, "Validating call event details: " + logMessage + ". Creating RAP file");
//	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
//	returnDetail->present = ReturnDetail_PR_severeReturn;
//	OCTET_STRING_fromBuf(&returnDetail->choice.severeReturn.fileSequenceNumber, (const char*) m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
//		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
//	returnDetail->choice.severeReturn.callEventDetail.
//	
//	//Copy AccountingInfo fields to Return Batch structure
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.currencyConversionInfo = 
//		m_transferBatch->accountingInfo->currencyConversionInfo;
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.discounting = 
//		m_transferBatch->accountingInfo->discounting;
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.localCurrency = 
//		m_transferBatch->accountingInfo->localCurrency;
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.tapCurrency = 
//		m_transferBatch->accountingInfo->tapCurrency;
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.tapDecimalPlaces = 
//		m_transferBatch->accountingInfo->tapDecimalPlaces;
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.taxation = 
//		m_transferBatch->accountingInfo->taxation;
//		
//	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
//	errorDetail->errorCode = errorCode;
//
//	// Fill Error Context List
//	int itemLevel = 1;
//	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
//	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
//	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
//	errorContext1level->itemLevel = itemLevel++;
//	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);
//
//	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
//	errorContext2level->pathItemId = asn_DEF_AccountingInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
//	errorContext2level->itemLevel = itemLevel++;
//	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);
//	
//	for (auto it = asnItems.cbegin() ; it != asnItems.cend(); it++)
//	{
//		ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
//		errorContext->pathItemId = (*it).m_asnType->tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
//		errorContext->itemLevel = itemLevel++;
//		if ((*it).m_itemOccurence > 0) {
//			errorContext->itemOccurrence = (ItemOccurrence_t*) calloc(1, sizeof(ItemOccurrence_t));
//			*errorContext->itemOccurrence = (*it).m_itemOccurence;
//		}
//		ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
//	}
//	ASN_SEQUENCE_ADD(&returnDetail->choice.severeReturn.accountingInfoError->errorDetail, errorDetail);
//
//	ReturnBatch* returnBatch = (ReturnBatch*) calloc(1, sizeof(ReturnBatch));
//	RAPFile rapFile(m_otlConnect, m_config);
//	
//	assert(m_transferBatch->batchControlInfo->sender);
//	assert(m_transferBatch->batchControlInfo->recipient);
//	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);
//	int loadRes = rapFile.CreateRAPFile(returnBatch, returnDetail, m_roamingHubID, (char*)m_transferBatch->batchControlInfo->sender->buf,
//		(char*) m_transferBatch->batchControlInfo->recipient->buf, (char*) m_transferBatch->batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf,
//		(m_transferBatch->batchControlInfo->fileTypeIndicator ? (char*) m_transferBatch->batchControlInfo->fileTypeIndicator->buf : ""),
//		m_rapFileID, m_rapSequenceNum);
//	
//	// Clear previously copied pointers to avoid ASN_STRUCT_FREE errors
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.currencyConversionInfo = NULL;
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.discounting = NULL;
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.localCurrency = NULL;
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.tapCurrency = NULL;
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.tapDecimalPlaces = NULL;
//	returnDetail->choice.severeReturn.accountingInfoError->accountingInfo.taxation = NULL;
//	ASN_STRUCT_FREE(asn_DEF_ReturnBatch, returnBatch);
//
//	return loadRes;
//}


long CallValidator::ValidateCallIOT(long long eventID, CallTypeForValidation callType, bool needRAP)
{
	otl_nocommit_stream otlStream;
	switch (callType) {
	case TELEPHONY_CALL:
		otlStream.open(1, "call BILLING.TAP3_IOT.ValidateTapCall(:event_id /*bigint,in*/, :err_descr /*char[255],out*/) "
			"into :res /*long,out*/", m_otlConnect);
		break;
	case GPRS_CALL:
		otlStream.open(1, "call BILLING.TAP3_IOT.ValidateGPRSCall(:event_id /*bigint,in*/, :err_descr /*char[255],out*/) "
			"into :res /*long,out*/", m_otlConnect);
		break;
	default:
		return VALIDATION_IMPOSSIBLE;
	}
	otlStream << eventID;
	long res;
	string errorDescr;
	otlStream
		>> errorDescr
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
		if(needRAP) {
			// update TAP3_Call set cancel_rap_seq_num = ...
		}
	}
	return res;
}
