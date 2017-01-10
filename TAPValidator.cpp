// Class TAPValidator checks TAP file (DataInterchange structure) validity according to TD.57 requirements.
// If Fatal or Severe errors are found it creates RAP file (Return Batch structure) and registers it in DB tables (RAP_File, RAP_Fatal_Return and so on).

#include <math.h>
#include <set>
#include <map>
#include "OTL_Header.h"
#include "TAP_Constants.h"
#include "DataInterchange.h"
#include "BatchControlInfo.h"
#include "ReturnBatch.h"
#include "ConfigContainer.h"
#include "TAPValidator.h"
#include "CallValidator.h"
#include "RAPFile.h"

using namespace std;

#define NO_ASN_ITEMS	vector<ErrContextAsnItem>()

extern void log(string filename, short msgType, string msgText, string dbConnectString = "");
extern void log(short msgType, string msgText, string dbConnectString = "");
extern long long OctetStr2Int64(const OCTET_STRING_t& octetStr);
extern int write_out(const void *buffer, size_t size, void *app_key);
extern "C" int ncftp_main(int argc, char **argv, char* result);


TAPValidator::TAPValidator(otl_connect& dbConnect, Config& config, long roamingHubID) 
	: m_otlConnect(dbConnect), m_config(config), m_roamingHubID(roamingHubID), m_rapFile(dbConnect, config, roamingHubID)
{
}


bool TAPValidator::IsRecipientCorrect(string recipient)
{
	otl_nocommit_stream otlStream;
	string ourTAPCode;
	otlStream.open(1, "call BILLING.TAP3.GetOurTAPCode(:roamhubid /*long,in*/) into :ourTapCode /*char[20],out*/" , m_otlConnect);
	otlStream << m_roamingHubID;
	otlStream
		>> ourTAPCode;
	otlStream.close();
	return (recipient == ourTAPCode);
}


bool TAPValidator::SetSenderNetworkID()
{
	otl_nocommit_stream otlStream;
	otlStream.open(1, "call BILLING.TAP3.GetSenderNetworkID(:sender /*char[20],in*/) "
		"into :res /*long,out*/", m_otlConnect);
	if (m_transferBatch) {
		otlStream << m_transferBatch->batchControlInfo->sender->buf;
	}
	else {
		otlStream << m_notification->sender->buf;
	}
	otlStream >> m_mobileNetworkID;
	otlStream.close();
	if (m_mobileNetworkID < 0) {
		return false;
	}
	return true;
}


void TAPValidator::SetIOTValidationMode()
{
	otl_nocommit_stream otlStream;
	otlStream.open(1, "select nvl(iot_validation_mode_id, :iot_no_need/*int,in*/) from BILLING.TMobileNetwork "
		"where object_no = :mobilenetworkid /*long,in*/", m_otlConnect);
	otlStream 
		<< IOT_NO_NEED
		<< m_mobileNetworkID;
	otlStream >> m_iotValidationMode;

}

FileDuplicationCheckRes TAPValidator::IsFileDuplicated()
{
	otl_nocommit_stream otlStream;
	otlStream.open(1, "call BILLING.TAP3.IsTAPFileDuplicated("
		":sender /*long,in*/, :recipient /*char[20],in*/, :roam_hub_id /*long,in*/, :file_seqnum /*char[20],in*/, "
		":file_type_indic /*char[20],in*/, :rap_file_seqnum /*char[20],in*/, :notif /*short,in*/, to_date(:avail_stamp /*char[20],in*/,'yyyymmddhh24miss'),"
		":event_count /*long,in*/, :total_charge /*double,in*/ ) "
		"into :res /*long,out*/", m_otlConnect);
	if (m_transferBatch) {
		double tapPower=pow( (double) 10, *m_transferBatch->accountingInfo->tapDecimalPlaces);
		otlStream
			<< m_mobileNetworkID
			<< m_transferBatch->batchControlInfo->recipient->buf
			<< m_roamingHubID
			<< m_transferBatch->batchControlInfo->fileSequenceNumber->buf
			<< ( m_transferBatch->batchControlInfo->fileTypeIndicator ? 
					(char*)m_transferBatch->batchControlInfo->fileTypeIndicator->buf : "" )
			<< ( m_transferBatch->batchControlInfo->rapFileSequenceNumber ? 
					(char*) m_transferBatch->batchControlInfo->rapFileSequenceNumber->buf : "" )
			<< (short) 0 /* transfer batch */
			 << m_transferBatch->batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf
			<< ( m_transferBatch->auditControlInfo->callEventDetailsCount ? *m_transferBatch->auditControlInfo->callEventDetailsCount : 0L )
			<< OctetStr2Int64(*m_transferBatch->auditControlInfo->totalCharge) / tapPower;
	}
	else {
		otlStream
			<< m_mobileNetworkID
			<< m_notification->recipient->buf
			<< m_roamingHubID
			<< m_notification->fileSequenceNumber->buf
			<< ( m_notification->fileTypeIndicator ? (char*) m_notification->fileTypeIndicator->buf : "" )
			<< ( m_notification->rapFileSequenceNumber ? (char*) m_notification->rapFileSequenceNumber->buf : "" )
			<< (short) 1 /* notification */
			<< m_notification->fileAvailableTimeStamp->localTimeStamp->buf
			<< 0L
			<< 0.0;
	}

	long result;
	otlStream >> result;
	otlStream.close();
	if(result < 0)
	{
		SetErrorAndLog("Функция TAP3.IsTAPFileDuplicated вернула ошибку " + to_string((long long) result));
		result = DUPLICATION_CHECK_ERROR;
	}
	return (FileDuplicationCheckRes) result;
}


IncomingTAPAllowed TAPValidator::IsIncomingTAPAllowed()
{
	if (IsTestFile())
		return INCOMING_TAP_ALLOWED;
	otl_nocommit_stream otlStream;
	otlStream.open(1, "call BILLING.TAP3.IsIncomingTAPAllowed("
		":sender /*long,in*/, :roam_hub_id /*long,in*/, to_date(:avail_stamp /*char[20],in*/,'yyyymmddhh24miss'))"
		"into :res /*long,out*/", m_otlConnect);
	if (m_transferBatch) {
		otlStream
			<< m_mobileNetworkID
			<< m_roamingHubID
			<< m_transferBatch->batchControlInfo->fileAvailableTimeStamp->localTimeStamp->buf;
	}
	else {
		otlStream
			<< m_mobileNetworkID
			<< m_roamingHubID
			<< m_notification->fileAvailableTimeStamp->localTimeStamp->buf;
	}
	long result;
	otlStream >> result;
	otlStream.close();
	if(result < 0)
	{
		SetErrorAndLog("Функция TAP3.IsIncomingTAPAllowed вернула ошибку " + to_string((long long) result));
		result = UNABLE_TO_DETERMINE;
	}
	return (IncomingTAPAllowed) result;
}

bool TAPValidator::IsTestFile()
{
	if (m_transferBatch) {
		if (m_transferBatch->batchControlInfo->fileTypeIndicator) {
			return !strncmp((char*)m_transferBatch->batchControlInfo->fileTypeIndicator->buf, "T", 1);
		}
	}
	else {
		if (m_notification->fileTypeIndicator) {
			return !strncmp((char*)m_notification->fileTypeIndicator->buf, "T", 1);
		}
	}
	return false;
}


bool TAPValidator::BatchContainsTaxes()
{
	for (int call_index = 0; call_index < m_transferBatch->callEventDetails->list.count; call_index++) {
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileOriginatedCall) {
			MobileOriginatedCall* moCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileOriginatedCall;
			for (int bs_used_index = 0; bs_used_index < moCall->basicServiceUsedList->list.count; bs_used_index++) 
				for (int chr_index = 0; chr_index < moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++)
					if (moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]->taxInformation != NULL)
						return true;
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileTerminatedCall) {
			MobileTerminatedCall* mtCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileTerminatedCall;
			for (int bs_used_index = 0; bs_used_index < mtCall->basicServiceUsedList->list.count; bs_used_index++) 
				for (int chr_index = 0; chr_index < mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++)
					if (mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]->taxInformation != NULL)
						return true;
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_gprsCall) {
			GprsCall* gprsCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.gprsCall;
			for (int chr_index = 0; chr_index < gprsCall->gprsServiceUsed->chargeInformationList->list.count; chr_index++)
				if (gprsCall->gprsServiceUsed->chargeInformationList->list.array[chr_index]->taxInformation != NULL)
					return true;
		}
	}
	return false;
}

bool TAPValidator::BatchContainsDiscounts()
{
	for (int call_index = 0; call_index < m_transferBatch->callEventDetails->list.count; call_index++) {
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileOriginatedCall) {
			MobileOriginatedCall* moCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileOriginatedCall;
			for (int bs_used_index = 0; bs_used_index < moCall->basicServiceUsedList->list.count; bs_used_index++) 
				for (int chr_index = 0; chr_index < moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++)
					if (moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]->discountInformation != NULL)
						return true;
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileTerminatedCall) {
			MobileTerminatedCall* mtCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileTerminatedCall;
			for (int bs_used_index = 0; bs_used_index < mtCall->basicServiceUsedList->list.count; bs_used_index++) 
				for (int chr_index = 0; chr_index < mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++)
					if (mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]->discountInformation != NULL)
						return true;
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_gprsCall) {
			GprsCall* gprsCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.gprsCall;
			for (int chr_index = 0; chr_index < gprsCall->gprsServiceUsed->chargeInformationList->list.count; chr_index++)
				if (gprsCall->gprsServiceUsed->chargeInformationList->list.array[chr_index]->discountInformation != NULL)
					return true;
		}
	}
	return false;
}

bool TAPValidator::ChargeInfoContainsPositiveCharges(ChargeInformation* chargeInfo)
{
	double tapPower = pow( (double) 10, *m_transferBatch->accountingInfo->tapDecimalPlaces);
	for (int chr_det_index = 0; chr_det_index < chargeInfo->chargeDetailList->list.count; chr_det_index++) {
		double charge = OctetStr2Int64(*chargeInfo->chargeDetailList->list.array[chr_det_index]->charge) / tapPower;
		if (charge > 0)
			return true;
	}
	return false;
}

bool TAPValidator::BatchContainsPositiveCharges()
{
	for (int call_index = 0; call_index < m_transferBatch->callEventDetails->list.count; call_index++) {
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileOriginatedCall) {
			MobileOriginatedCall* moCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileOriginatedCall;
			for (int bs_used_index = 0; bs_used_index < moCall->basicServiceUsedList->list.count; bs_used_index++)
				for (int chr_index = 0; chr_index < moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++)
					if (ChargeInfoContainsPositiveCharges(
							moCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]))
						return true;
		}
		
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileTerminatedCall) {
			MobileTerminatedCall* mtCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileTerminatedCall;
			for (int bs_used_index = 0; bs_used_index < mtCall->basicServiceUsedList->list.count; bs_used_index++) 
				for (int chr_index = 0; chr_index < mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.count; chr_index++) 
					if (ChargeInfoContainsPositiveCharges(
							mtCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList->list.array[chr_index]))
						return true;
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_gprsCall) {
			GprsCall* gprsCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.gprsCall;
			for (int chr_index = 0; chr_index < gprsCall->gprsServiceUsed->chargeInformationList->list.count; chr_index++)
				if (ChargeInfoContainsPositiveCharges(
						gprsCall->gprsServiceUsed->chargeInformationList->list.array[chr_index]))
					return true;
		}
	}
	return false;
}


long long TAPValidator::ChargeInfoListTotalCharge(ChargeInformationList* chargeInfoList)
{
	long long totalCharge = 0;
	for (int chr_index = 0; chr_index < chargeInfoList->list.count; chr_index++)
		for (int chr_det_index = 0; chr_det_index < chargeInfoList->list.array[chr_index]->chargeDetailList->list.count; chr_det_index++) {
			if (string((char*) chargeInfoList->list.array[chr_index]->chargeDetailList->list.array[chr_det_index]->chargeType->buf) == "00")
				totalCharge += OctetStr2Int64(*chargeInfoList->list.array[chr_index]->chargeDetailList->list.array[chr_det_index]->charge);
	}
	return totalCharge;
}


long long TAPValidator::BatchTotalCharge()
{
	long long totalCharge = 0;
	for (int call_index = 0; call_index < m_transferBatch->callEventDetails->list.count; call_index++) {
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileOriginatedCall) {
			MobileOriginatedCall* mCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileOriginatedCall;
			for (int bs_used_index = 0; bs_used_index < mCall->basicServiceUsedList->list.count; bs_used_index++)
				totalCharge += ChargeInfoListTotalCharge(mCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList);
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileTerminatedCall) {
			MobileTerminatedCall* mCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileTerminatedCall;
			for (int bs_used_index = 0; bs_used_index < mCall->basicServiceUsedList->list.count; bs_used_index++)
				totalCharge += ChargeInfoListTotalCharge(mCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList);
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_gprsCall) {
			GprsCall* mCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.gprsCall;
			totalCharge += ChargeInfoListTotalCharge(mCall->gprsServiceUsed->chargeInformationList);
		}
	}
	return totalCharge;
}


int TAPValidator::CreateAndUploadRapFile(ReturnDetail* returnDetail)
{
	assert(m_transferBatch || m_notification);
	try {
		if (m_transferBatch) {
			m_rapFile.Initialize(m_transferBatch);
		}
		else {
			m_rapFile.Initialize(m_notification);
		}
		m_rapFile.AddReturnDetail(returnDetail, 0);
		m_rapFile.Finalize();
		m_rapFile.LoadToDB();
		return m_rapFile.EncodeAndUpload();
	}
	catch(const RAPFileException& ex) {
		SetErrorAndLog(string(ex.what()));
		return TL_TAP_NOT_VALIDATED;
	}
}


int TAPValidator::CreateBatchControlInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems)
{
	SetErrorAndLog("Валидация Batch Control Info: " + logMessage);
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, 
		(const char*) m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.batchControlError = (BatchControlError*) calloc(1, sizeof(BatchControlError));
	
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo = *m_transferBatch->batchControlInfo;
	
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	int itemLevel = 1;
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext1level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext2level->pathItemId = asn_DEF_BatchControlInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext2level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);

	for (auto it = asnItems.cbegin() ; it != asnItems.cend(); it++)
	{
		ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
		errorContext->pathItemId = (*it).m_asnType->tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
		errorContext->itemLevel = itemLevel++;
		if ((*it).m_itemOccurence > 0) {
			errorContext->itemOccurrence = (ItemOccurrence_t*) calloc(1, sizeof(ItemOccurrence_t));
			*errorContext->itemOccurrence = (*it).m_itemOccurence;
		}
		ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.batchControlError->errorDetail, errorDetail);

	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->recipient);
	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);

	int loadRes = CreateAndUploadRapFile(returnDetail);

	// Clear previously copied pointers to avoid ASN_STRUCT_FREE errors
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.sender = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.recipient = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileAvailableTimeStamp = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileCreationTimeStamp = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.transferCutOffTimeStamp = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileSequenceNumber = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.fileTypeIndicator = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.operatorSpecInformation = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.rapFileSequenceNumber = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.releaseVersionNumber = NULL;
	returnDetail->choice.fatalReturn.batchControlError->batchControlInfo.specificationVersionNumber = NULL;
	
	return loadRes;
}

TAPValidationResult TAPValidator::FileSequenceNumberControl()
{
	FileDuplicationCheckRes checkRes = IsFileDuplicated();
	int createRapRes;
	vector<ErrContextAsnItem> asnItems;
	switch (checkRes) {
		case DUPLICATION_NOTFOUND:
			return TAP_VALID;
		case COPY_FOUND:
			log(LOG_INFO, "Файл, имеющий тот же последовательный номер, уже был загружен и обработан. "
				"Повторной загрузки не требуется.");
			return FILE_DUPLICATION;
		case DUPLICATION_FOUND:
			asnItems.push_back(ErrContextAsnItem(&asn_DEF_FileSequenceNumber, 0));
			createRapRes = CreateBatchControlInfoRAPFile(
				"Дублирование: Файл, имеющий тот же последовательный номер, уже был загружен и обработан.",
				FILE_SEQ_NUM_DUPLICATION, asnItems);
			return ( createRapRes >= 0 ? FILE_DUPLICATION : VALIDATION_IMPOSSIBLE );
		case DUPLICATION_CHECK_ERROR:
		default:
			return VALIDATION_IMPOSSIBLE;
	}
}

TAPValidationResult TAPValidator::ValidateBatchControlInfo()
{
	if (!m_transferBatch->batchControlInfo->sender || !m_transferBatch->batchControlInfo->recipient || 
			!m_transferBatch->batchControlInfo->fileSequenceNumber) {
		SetErrorAndLog(std::string("Валидация: Sender, Recipient или FileSequenceNumber отсутствуют в Batch Control Info."));
		return VALIDATION_IMPOSSIBLE;
	}

	if(!IsRecipientCorrect((char*) m_transferBatch->batchControlInfo->recipient->buf)) {
		SetErrorAndLog("Валидация: Некорректный получатель " + string((char*) m_transferBatch->batchControlInfo->
			recipient->buf) + " Файл адресован другому получателю.");
		return VALIDATION_IMPOSSIBLE;
	}

	if (!m_transferBatch->batchControlInfo->fileAvailableTimeStamp) {
		int createRapRes = CreateBatchControlInfoRAPFile("fileAvailableTimeStamp отсутствует в Batch Control Info", 
			BATCH_CTRL_FILE_AVAIL_TIMESTAMP_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->batchControlInfo->specificationVersionNumber) {
		int createRapRes = CreateBatchControlInfoRAPFile("specificationVersionNumber отсутствует в Batch Control Info", 
			BATCH_CTRL_SPEC_VERSION_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->batchControlInfo->transferCutOffTimeStamp) {
		int createRapRes = CreateBatchControlInfoRAPFile("transferCutOffTimeStamp отсутствует в Batch Control Info", 
			BATCH_CTRL_TRANSFER_CUTOFF_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}

	int fileSeqNum;
	try {
		fileSeqNum = stoi((char*)m_transferBatch->batchControlInfo->fileSequenceNumber->buf, NULL);
	}
	catch(const std::invalid_argument&) {
		// wrong file sequence number given
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_FileSequenceNumber, 0));
		int createRapRes = CreateBatchControlInfoRAPFile("Указан нечисловой fileSequenceNumber", 
			FILE_SEQ_NUM_SYNTAX_ERROR, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	
	if (!(fileSeqNum >= START_TAP_SEQUENCE_NUM && fileSeqNum <= END_TAP_SEQUENCE_NUM)) {
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_FileSequenceNumber, 0));
		int createRapRes = CreateBatchControlInfoRAPFile("fileSequenceNumber вне допустимого диапазона", 
			FILE_SEQ_NUM_OUT_OF_RANGE, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}

	return FileSequenceNumberControl();
}


int TAPValidator::CreateAccountingInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems)
{
	SetErrorAndLog("Валидация Accounting Info: " + logMessage);
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, (const char*) m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.accountingInfoError = (AccountingInfoError*) calloc(1, sizeof(AccountingInfoError));
	
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo = *m_transferBatch->accountingInfo;
		
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	int itemLevel = 1;
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext1level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext2level->pathItemId = asn_DEF_AccountingInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext2level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);
	
	for (auto it = asnItems.cbegin() ; it != asnItems.cend(); it++)
	{
		ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
		errorContext->pathItemId = (*it).m_asnType->tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
		errorContext->itemLevel = itemLevel++;
		if ((*it).m_itemOccurence > 0) {
			errorContext->itemOccurrence = (ItemOccurrence_t*) calloc(1, sizeof(ItemOccurrence_t));
			*errorContext->itemOccurrence = (*it).m_itemOccurence;
		}
		ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.accountingInfoError->errorDetail, errorDetail);

	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->recipient);
	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);

	int loadRes = CreateAndUploadRapFile(returnDetail);
	
	// Clear previously copied pointers to avoid ASN_STRUCT_FREE errors
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.currencyConversionInfo = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.discounting = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.localCurrency = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.tapCurrency = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.tapDecimalPlaces = NULL;
	returnDetail->choice.fatalReturn.accountingInfoError->accountingInfo.taxation = NULL;
	
	return loadRes;
}


ExRateValidationRes TAPValidator::ValidateChrInfoExRates(ChargeInformationList* pChargeInfoList, LocalTimeStamp_t* pCallTimestamp,
		const map<ExchangeRateCode_t, double>& exchangeRates, string tapLocalCurrency)
{
	long long eventCharge = ChargeInfoListTotalCharge(pChargeInfoList);
	if(eventCharge > 0) {
		// validate exchange rate
		for(int chrinfo_index = 0; chrinfo_index < pChargeInfoList->list.count; chrinfo_index++) {
			if (pChargeInfoList->list.array[chrinfo_index]->exchangeRateCode) {
				map<ExchangeRateCode_t, double>::const_iterator it =
					exchangeRates.find(*pChargeInfoList->list.array[chrinfo_index]->exchangeRateCode);
				if (it != exchangeRates.end()) {
					otl_nocommit_stream otlStream;
					otlStream.open(1, "CALL BILLING.TAP3.ValidateExchangeRate(:mobnetworkid /*long,in*/, "
						":currency /*char[10],in*/, to_date(:call_time /*char[20],in*/,'yyyymmddhh24miss'), :ex_rate /*double,in*/) "
						"into :res /*long,out*/", m_otlConnect);
					otlStream
						<< m_mobileNetworkID
						<< tapLocalCurrency
						<< pCallTimestamp->buf
						<< it->second;
					long validationRes;
					otlStream >> validationRes;
					otlStream.close();
					if (validationRes != EXRATE_VALID) {
						// break processing and return error
						return (ExRateValidationRes)validationRes;
					}
				}
				else {
					return EXRATE_WRONG_CODE;
				}
			}
		}
	}
	return EXRATE_VALID;
}

ExRateValidationRes TAPValidator::ValidateExchangeRates(const map<ExchangeRateCode_t, double>& exchangeRates, string tapLocalCurrency)
{
	long long eventCharge = 0;
	ChargeInformationList* pChargeInfoList;
	otl_nocommit_stream otlStream;
	for (int call_index = 0; call_index < m_transferBatch->callEventDetails->list.count; call_index++) {
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileOriginatedCall) {
			MobileOriginatedCall* mCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileOriginatedCall;
			for (int bs_used_index = 0; bs_used_index < mCall->basicServiceUsedList->list.count; bs_used_index++) {
				pChargeInfoList = mCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList;
				ExRateValidationRes validationRes = ValidateChrInfoExRates(pChargeInfoList, 
					mCall->basicCallInformation->callEventStartTimeStamp->localTimeStamp, exchangeRates, tapLocalCurrency);
				if (validationRes != EXRATE_VALID) {
					// break processing and return error
					return (ExRateValidationRes)validationRes;
				}
			}
		}
		if(m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_mobileTerminatedCall) {
			MobileTerminatedCall* mCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.mobileTerminatedCall;
			for (int bs_used_index = 0; bs_used_index < mCall->basicServiceUsedList->list.count; bs_used_index++) {
				pChargeInfoList = mCall->basicServiceUsedList->list.array[bs_used_index]->chargeInformationList;
				ExRateValidationRes validationRes = ValidateChrInfoExRates(pChargeInfoList, 
					mCall->basicCallInformation->callEventStartTimeStamp->localTimeStamp, exchangeRates, tapLocalCurrency);
				if (validationRes != EXRATE_VALID) {
					// break processing and return error
					return (ExRateValidationRes)validationRes;
				}
			}
		}
		if (m_transferBatch->callEventDetails->list.array[call_index]->present == CallEventDetail_PR_gprsCall) {
			GprsCall* mCall = &m_transferBatch->callEventDetails->list.array[call_index]->choice.gprsCall;
			pChargeInfoList = mCall->gprsServiceUsed->chargeInformationList;
			ExRateValidationRes validationRes = ValidateChrInfoExRates(pChargeInfoList,
				mCall->gprsBasicCallInformation->callEventStartTimeStamp->localTimeStamp, exchangeRates, tapLocalCurrency);
			if (validationRes != EXRATE_VALID) {
				// break processing and return error
				return (ExRateValidationRes)validationRes;
			}
		}
	}
	return EXRATE_VALID;
}


TAPValidationResult TAPValidator::ValidateAccountingInfo()
{
	if (!m_transferBatch->accountingInfo->localCurrency) {
		int createRapRes = CreateAccountingInfoRAPFile("localCurrency is missing in Accounting Info", 
			ACCOUNTING_LOCAL_CURRENCY_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->accountingInfo->tapDecimalPlaces) {
		int createRapRes = CreateAccountingInfoRAPFile("tapDecimalPlaces is missing in Accounting Info", 
			ACCOUNTING_TAP_DECIMAL_PLACES_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->accountingInfo->taxation && BatchContainsTaxes()) {
		int createRapRes = CreateAccountingInfoRAPFile(
			"taxation group is missing in Accounting Info and batch contains taxes", 
			ACCOUNTING_TAXATION_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->accountingInfo->discounting && BatchContainsDiscounts()) {
		int createRapRes = CreateAccountingInfoRAPFile(
			"discounting group is missing in Accounting Info and batch contains discounts", 
			ACCOUNTING_DISCOUNTING_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->accountingInfo->currencyConversionInfo && BatchContainsPositiveCharges()) {
		int createRapRes = CreateAccountingInfoRAPFile(
			"currencyConversion group is missing in Accounting Info and batch contains charges greater than 0",
			ACCOUNTING_CURRENCY_CONVERSION_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	// Validating currency conversion table
	if (m_transferBatch->accountingInfo->currencyConversionInfo) {
		map<ExchangeRateCode_t, double> exchangeRates;
		vector<ErrContextAsnItem> asnItems;
		for (int i = 0; i < m_transferBatch->accountingInfo->currencyConversionInfo->list.count; i++) {
			asnItems.push_back(ErrContextAsnItem(&asn_DEF_CurrencyConversionList, i + 1));
			if (!m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->exchangeRateCode) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"Mandatory item Exchange Rate Code missing within group Currency Conversion",
					CURRENCY_CONVERSION_EXRATE_CODE_MISSING, asnItems);
				return (createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			if (!m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->numberOfDecimalPlaces) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"Mandatory item Exchange Rate Code missing within group Currency Conversion",
					CURRENCY_CONVERSION_NUM_OF_DEC_PLACES_MISSING, asnItems);
				return (createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			if (!m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->exchangeRate) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"Mandatory item Exchange Rate Code missing within group Currency Conversion",
					CURRENCY_CONVERSION_EXCHANGE_RATE_MISSING, asnItems);
				return (createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			// check exchange rate code duplication
			if (exchangeRates.find(*m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->exchangeRateCode) !=
				exchangeRates.end()) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"More than one occurrence of group with same Exchange Rate Code within group Currency Conversion",
					CURRENCY_CONVERSION_EXRATE_CODE_DUPLICATION, asnItems);
				return (createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			else {
				exchangeRates.insert(pair<ExchangeRateCode_t, double>
					(*m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->exchangeRateCode,
					*m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->exchangeRate /
					pow((double)10, *m_transferBatch->accountingInfo->currencyConversionInfo->list.array[i]->numberOfDecimalPlaces)));
			}
		}
		ExRateValidationRes validationRes = ValidateExchangeRates(exchangeRates, 
			(char*) m_transferBatch->accountingInfo->localCurrency->buf);
		if(validationRes != EXRATE_VALID) {
			int createRapRes;
			string logMessage;
			switch (validationRes) {
				case EXRATE_HIGHER:
				case EXRATE_LOWER:
					createRapRes = CreateAccountingInfoRAPFile(
						validationRes == EXRATE_HIGHER ?
							"Exchange Rate higher than expected and applied to one or more Charges" :
							"Exchange Rate less than expected and applied to one or more Charges",
						validationRes == EXRATE_HIGHER ?
							ACCOUNTING_EXCHARGE_RATE_HIGHER :
							ACCOUNTING_EXCHARGE_RATE_LOWER,
						asnItems);
					return (createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
				case EXRATE_WRONG_CODE:
					logMessage = "Wrong exchange rate code given";
					break;
				case EXRATE_CURRENCY_NOT_FOUND:
					logMessage = string("TAP currency \"") + (char*) m_transferBatch->accountingInfo->localCurrency->buf  + 
						"\" is not found in IRBiS";
					break;
				case EXRATE_CURRENCY_MISMATCH:
					logMessage = string("IRBiS currency setting for this network differs from \"") + 
						(char*) m_transferBatch->accountingInfo->localCurrency->buf  + 
						"\" given in TAP file";
					break;
				case EXRATE_NOT_SET:
					logMessage = string("Exchange rate for \"") + (char*) m_transferBatch->accountingInfo->localCurrency->buf  + 
						"\" is not set in IRBiS";
					break;
				}
			SetErrorAndLog("Невозможно проверить курсы обмена валют, код ошибки " + to_string((long long) validationRes) +
				" (" + logMessage + ")");
			return VALIDATION_IMPOSSIBLE;
		}
	}

	if (!(*m_transferBatch->accountingInfo->tapDecimalPlaces >= TAP_DECIMAL_VALID_FROM 
			&& *m_transferBatch->accountingInfo->tapDecimalPlaces <= TAP_DECIMAL_VALID_TO)) {
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_TapDecimalPlaces, 0));
		int createRapRes = CreateAccountingInfoRAPFile("TAP Decimal Places is out of range ( " + 
			to_string((long long) *m_transferBatch->accountingInfo->tapDecimalPlaces) + " value given).", 
			TAP_DECIMAL_OUT_OF_RANGE, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}

	// Validating taxation table
	if (m_transferBatch->accountingInfo->taxation) {
		set<ExchangeRateCode_t> taxRateCodes;
		for (int i = 0; i < m_transferBatch->accountingInfo->taxation->list.count; i++) {
			vector<ErrContextAsnItem> asnItems;
			asnItems.push_back(ErrContextAsnItem(&asn_DEF_TaxationList, i + 1));
			if (!m_transferBatch->accountingInfo->taxation->list.array[i]->taxCode) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"Mandatory item Tax Rate Code missing within group Taxation",
					TAXATION_TAXRATE_CODE_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			if (!m_transferBatch->accountingInfo->taxation->list.array[i]->taxType) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"Mandatory item Exchange Rate Code missing within group Taxation",
					TAXATION_TAX_TYPE_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			// check tax code duplication
			if (taxRateCodes.find(*m_transferBatch->accountingInfo->taxation->list.array[i]->taxCode) !=
					taxRateCodes.end()) {
				int createRapRes = CreateAccountingInfoRAPFile(
					"More than one occurrence of group with same Tax Rate Code within group Taxation",
					CURRENCY_CONVERSION_EXRATE_CODE_DUPLICATION, asnItems);
				return ( createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE );
			}
			else {
				taxRateCodes.insert(*m_transferBatch->accountingInfo->taxation->list.array[i]->taxCode);
			}
			if (m_transferBatch->accountingInfo->taxation->list.array[i]->taxRate) {
				long taxRate;
				try {
					taxRate = stol((char*) m_transferBatch->accountingInfo->taxation->list.array[i]->taxRate->buf, NULL);
				}
				catch (const std::invalid_argument&) {
					// wrong tax rate given
					int createRapRes = CreateAccountingInfoRAPFile("Tax Rate is not a number (syntax error) at element " + to_string((long long) ( i + 1 )),
						TAX_RATE_SYNTAX_ERROR, asnItems);
					return ( createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE );
				}

				if (!( taxRate >= TAX_RATE_VALID_FROM && taxRate <= TAX_RATE_VALID_TO )) {
					vector<ErrContextAsnItem> asnItems;
					asnItems.push_back(ErrContextAsnItem(&asn_DEF_TaxationList, i + 1));
					asnItems.push_back(ErrContextAsnItem(&asn_DEF_Taxation, 0));
					asnItems.push_back(ErrContextAsnItem(&asn_DEF_TaxRate, 0));
					int createRapRes = CreateAccountingInfoRAPFile("Tax Rate is out of range at element " + to_string((long long) ( i + 1 )),
						TAX_RATE_OUT_OF_RANGE, asnItems);
					return ( createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE );
				}
			}
		}
	}

	return TAP_VALID;
}


int TAPValidator::CreateNetworkInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems)
{
	SetErrorAndLog("Валидация Network Info: " + logMessage);
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, (const char*) m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.networkInfoError = (NetworkInfoError*) calloc(1, sizeof(NetworkInfoError));
	
	//Copy NetworkInfo fields to Return Batch structure
	returnDetail->choice.fatalReturn.networkInfoError->networkInfo = *m_transferBatch->networkInfo;
		
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	int itemLevel = 1;
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext1level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext2level->pathItemId = asn_DEF_NetworkInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext2level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);

	for (auto it = asnItems.cbegin() ; it != asnItems.cend(); it++)	{
		ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
		errorContext->pathItemId = (*it).m_asnType->tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
		errorContext->itemLevel = itemLevel++;
		if ((*it).m_itemOccurence > 0) {
			errorContext->itemOccurrence = (ItemOccurrence_t*) calloc(1, sizeof(ItemOccurrence_t));
			*errorContext->itemOccurrence = (*it).m_itemOccurence;
		}
		ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.networkInfoError->errorDetail, errorDetail);

	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->recipient);
	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);

	int loadRes = CreateAndUploadRapFile(returnDetail);

	// Clear previously copied pointers to avoid ASN_STRUCT_FREE errors
	returnDetail->choice.fatalReturn.networkInfoError->networkInfo.recEntityInfo = NULL;
	returnDetail->choice.fatalReturn.networkInfoError->networkInfo.utcTimeOffsetInfo = NULL;
	
	return loadRes;
}


TAPValidationResult TAPValidator::ValidateNetworkInfo()
{
	// check mandatory structures in Transfer Batch/Network Information
	if (!m_transferBatch->networkInfo->utcTimeOffsetInfo) {
		int createRapRes = CreateNetworkInfoRAPFile("utcTimeOffsetInfo is missing in Network Info",
			NETWORK_UTC_TIMEOFFSET_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->networkInfo->recEntityInfo) {
		int createRapRes = CreateNetworkInfoRAPFile("recEntityInfo is missing in Network Info",
			NETWORK_REC_ENTITY_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	// Validating recording entities table
	if (m_transferBatch->networkInfo->recEntityInfo) {
		set<RecEntityCode_t> recEntityCodes;
		for (int i = 0; i < m_transferBatch->networkInfo->recEntityInfo->list.count; i++) {
			vector<ErrContextAsnItem> asnItems;
			asnItems.push_back(ErrContextAsnItem(&asn_DEF_RecEntityInfoList, i + 1));
			if (!m_transferBatch->networkInfo->recEntityInfo->list.array[i]->recEntityCode) {
				int createRapRes = CreateNetworkInfoRAPFile(
					"Mandatory item Recording Entity Code missing within group Network Information",
					REC_ENTITY_CODE_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			if (!m_transferBatch->networkInfo->recEntityInfo->list.array[i]->recEntityType) {
				int createRapRes = CreateNetworkInfoRAPFile(
					"Mandatory item Recording Entity Type missing within group Network Information",
					REC_ENTITY_TYPE_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			if (!m_transferBatch->networkInfo->recEntityInfo->list.array[i]->recEntityId) {
				int createRapRes = CreateNetworkInfoRAPFile(
					"Mandatory item Recording Entity Indentification missing within group Network Information",
					REC_ENTITY_IDENTIFICATION_MISSING, asnItems);
				return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
			}
			// check recording entity code duplication
			if (recEntityCodes.find(*m_transferBatch->networkInfo->recEntityInfo->list.array[i]->recEntityCode) !=
					recEntityCodes.end()) {
				int createRapRes = CreateNetworkInfoRAPFile(
					"More than one occurrence of group with same Recording Entity Code within group Recording Entity Information",
					REC_ENTITY_CODE_DUPLICATION, asnItems);
				return ( createRapRes >= 0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE );
			}
			else {
				recEntityCodes.insert(*m_transferBatch->networkInfo->recEntityInfo->list.array[i]->recEntityCode);
			}
		}
	}

	return TAP_VALID;
}


int TAPValidator::CreateAuditControlInfoRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems)
{
	SetErrorAndLog("Валидация Audit Control Info: " + logMessage);
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, (const char*) m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.auditControlInfoError = (AuditControlInfoError*) calloc(1, sizeof(AuditControlInfoError));
	
	//Copy auditControlInfo fields to Return Batch structure
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo = *m_transferBatch->auditControlInfo;
	
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	int itemLevel = 1;
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext1level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	ErrorContext* errorContext2level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext2level->pathItemId = asn_DEF_AuditControlInfo.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
	errorContext2level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext2level);
	
	for (auto it = asnItems.cbegin() ; it != asnItems.cend(); it++)	{
		ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
		errorContext->pathItemId = (*it).m_asnType->tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
		errorContext->itemLevel = itemLevel++;
		if ((*it).m_itemOccurence > 0) {
			errorContext->itemOccurrence = (ItemOccurrence_t*) calloc(1, sizeof(ItemOccurrence_t));
			*errorContext->itemOccurrence = (*it).m_itemOccurence;
		}
		ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.auditControlInfoError->errorDetail, errorDetail);

	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);

	int loadRes = CreateAndUploadRapFile(returnDetail);

	// Clear previously copied pointers to avoid ASN_STRUCT_FREE errors
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.callEventDetailsCount = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.earliestCallTimeStamp = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.latestCallTimeStamp = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.operatorSpecInformation = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalAdvisedChargeValueList = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalCharge = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalChargeRefund = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalDiscountRefund = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalDiscountValue = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalTaxRefund = NULL;
	returnDetail->choice.fatalReturn.auditControlInfoError->auditControlInfo.totalTaxValue = NULL;
	
	return loadRes;
}


TAPValidationResult TAPValidator::ValidateAuditControlInfo()
{
	// check mandatory structures in Transfer Batch/Audit Control Information
	if (!m_transferBatch->auditControlInfo->totalCharge) {
		int createRapRes = CreateAuditControlInfoRAPFile("totalCharge is missing in Audit Control Info",
			AUDIT_CTRL_TOTAL_CHARGE_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->auditControlInfo->totalTaxValue) {
		int createRapRes = CreateAuditControlInfoRAPFile("totalTaxValue is missing in Audit Control Info",
			AUDIT_CTRL_TOTAL_TAX_VALUE_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->auditControlInfo->totalDiscountValue) {
		int createRapRes = CreateAuditControlInfoRAPFile("totalDiscountValue is missing in Audit Control Info",
			AUDIT_CTRL_TOTAL_DISCOUNT_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->auditControlInfo->callEventDetailsCount) {
		int createRapRes = CreateAuditControlInfoRAPFile("callEventDetailsCount is missing in Audit Control Info",
			AUDIT_CTRL_CALL_COUNT_MISSING, NO_ASN_ITEMS);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (*m_transferBatch->auditControlInfo->callEventDetailsCount != m_transferBatch->callEventDetails->list.count) {
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_CallEventDetailsCount, 0));
		int createRapRes = CreateAuditControlInfoRAPFile(
			"CallEventDetailsCount does not match the count of Call Event Details",
			CALL_COUNT_MISMATCH, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}

	long long auditTotalCharge = OctetStr2Int64(*m_transferBatch->auditControlInfo->totalCharge);
	long long calcTotalCharge = BatchTotalCharge();
	if (OctetStr2Int64(*m_transferBatch->auditControlInfo->totalCharge) != BatchTotalCharge()) {
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_TotalCharge, 0));
		int createRapRes = CreateAuditControlInfoRAPFile(
			"Total charge does not match the calculated sum of non refund charges",
			TOTAL_CHARGE_MISMATCH, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	return TAP_VALID;
}


int TAPValidator::CreateTransferBatchRAPFile(string logMessage, int errorCode)
{
	SetErrorAndLog("Валидация Transfer Batch: " + logMessage);
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, (const char*)m_transferBatch->batchControlInfo->fileSequenceNumber->buf, 
		m_transferBatch->batchControlInfo->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.transferBatchError = (TransferBatchError*) calloc(1, sizeof(TransferBatchError));
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_TransferBatch.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it
	errorContext1level->itemLevel = 1;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.transferBatchError->errorDetail, errorDetail);
	
	assert(m_transferBatch->batchControlInfo->sender);
	assert(m_transferBatch->batchControlInfo->recipient);
	assert(m_transferBatch->batchControlInfo->fileAvailableTimeStamp);
	
	return CreateAndUploadRapFile(returnDetail);
}


TAPValidationResult TAPValidator::ValidateTransferBatch()
{
	assert(!m_notification);
	assert(m_transferBatch);
	
	// check mandatory structures in Transfer Batch
	if (!m_transferBatch->batchControlInfo) {
		int createRapRes = CreateTransferBatchRAPFile("Batch Control Info missing in Transfer Batch", 
			TF_BATCH_BATCH_CONTROL_INFO_MISSING);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!m_transferBatch->accountingInfo) {
		int createRapRes = CreateTransferBatchRAPFile("Accounting Info missing in Transfer Batch", 
			TF_BATCH_ACCOUNTING_INFO_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->networkInfo) {
		int createRapRes = CreateTransferBatchRAPFile("Network Info missing in Transfer Batch", 
			TF_BATCH_NETWORK_INFO_MISSING);
		return FATAL_ERROR;
	}
	if (!m_transferBatch->auditControlInfo) {
		int createRapRes = CreateTransferBatchRAPFile("Audit Control Info missing in Transfer Batch", 
			TF_BATCH_AUDIT_CONTROL_INFO_MISSING);
		return FATAL_ERROR;
	}
	
	// Validating transfer batch structures
	TAPValidationResult validationRes = ValidateBatchControlInfo();
	if (validationRes != TL_OK)
		return validationRes;

	validationRes = ValidateAccountingInfo();
	if (validationRes != TL_OK)
		return validationRes;

	// check mandatory structures in Transfer Batch/Network Information
	validationRes = ValidateNetworkInfo();
	if (validationRes != TL_OK)
		return validationRes;

	validationRes = ValidateAuditControlInfo();
	if (validationRes != TL_OK)
		return validationRes;
	
	return TAP_VALID;
}


int TAPValidator::CreateNotificationRAPFile(string logMessage, int errorCode, const vector<ErrContextAsnItem>& asnItems)
{
	SetErrorAndLog("Валидация Notification: " + logMessage);
	ReturnDetail* returnDetail = (ReturnDetail*) calloc(1, sizeof(ReturnDetail));
	returnDetail->present = ReturnDetail_PR_fatalReturn;
	OCTET_STRING_fromBuf(&returnDetail->choice.fatalReturn.fileSequenceNumber, (const char*) m_notification->fileSequenceNumber->buf, 
		m_notification->fileSequenceNumber->size);
	returnDetail->choice.fatalReturn.notificationError = (NotificationError*) calloc(1, sizeof(NotificationError));
	ErrorDetail* errorDetail = (ErrorDetail*) calloc(1, sizeof(ErrorDetail));
	errorDetail->errorCode = errorCode;

	// Fill Error Context List
	int itemLevel = 1;
	errorDetail->errorContext = (ErrorContextList*) calloc(1, sizeof(ErrorContextList));
	ErrorContext* errorContext1level = (ErrorContext*) calloc(1, sizeof(ErrorContext));
	errorContext1level->pathItemId = asn_DEF_Notification.tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it
	errorContext1level->itemLevel = itemLevel++;
	ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext1level);

	for (auto it = asnItems.cbegin() ; it != asnItems.cend(); it++)	{
		ErrorContext* errorContext = (ErrorContext*) calloc(1, sizeof(ErrorContext));
		errorContext->pathItemId = (*it).m_asnType->tags[0] >> 2; // 2 rightmost bits is TAG class at ASN1 compiler, remove it  
		errorContext->itemLevel = itemLevel++;
		if ((*it).m_itemOccurence > 0) {
			errorContext->itemOccurrence = (ItemOccurrence_t*) calloc(1, sizeof(ItemOccurrence_t));
			*errorContext->itemOccurrence = (*it).m_itemOccurence;
		}
		ASN_SEQUENCE_ADD(errorDetail->errorContext, errorContext);
	}
	ASN_SEQUENCE_ADD(&returnDetail->choice.fatalReturn.notificationError->errorDetail, errorDetail);
	
	assert(m_notification->sender);
	assert(m_notification->recipient);
	assert(m_notification->fileAvailableTimeStamp);
	
	return CreateAndUploadRapFile(returnDetail);
}


TAPValidationResult TAPValidator::ValidateNotification()
{
	assert(m_notification);
	assert(!m_transferBatch);
	if (!m_notification->sender || !m_notification->recipient || !m_notification->fileSequenceNumber) {
		SetErrorAndLog(std::string("Валидация: Sender, Recipient либо FileSequenceNumber отсутствует в Notification."));
		return VALIDATION_IMPOSSIBLE;
	}

	if(!IsRecipientCorrect((char*) m_notification->recipient->buf)) {
		SetErrorAndLog("Валидация: некорректный получатель " + string((char*) m_notification->recipient->buf));
		return VALIDATION_IMPOSSIBLE;
	}

	int fileSeqNum;
	try {
		fileSeqNum = stoi((char*) m_notification->fileSequenceNumber->buf, NULL);
	}
	catch(const std::invalid_argument&) {
		// wrong file sequence number given
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_FileSequenceNumber, 0));
		int createRapRes = CreateNotificationRAPFile("В файле указан нечисловой fileSequenceNumber", 
			FILE_SEQ_NUM_SYNTAX_ERROR, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	if (!(fileSeqNum >= START_TAP_SEQUENCE_NUM && fileSeqNum <= END_TAP_SEQUENCE_NUM)) {
		vector<ErrContextAsnItem> asnItems;
		asnItems.push_back(ErrContextAsnItem(&asn_DEF_FileSequenceNumber, 0));
		int createRapRes = CreateNotificationRAPFile("fileSequenceNumber вне допустимого диапазона", 
			FILE_SEQ_NUM_OUT_OF_RANGE, asnItems);
		return (createRapRes >=0 ? FATAL_ERROR : VALIDATION_IMPOSSIBLE);
	}
	return FileSequenceNumberControl();
}


void TAPValidator::Validate(DataInterChange* dataInterchange)
{
	switch (dataInterchange->present) {
		case DataInterChange_PR_transferBatch:
			m_transferBatch = &dataInterchange->choice.transferBatch;
			m_notification = NULL;
			break;
		case DataInterChange_PR_notification:
			m_notification = &dataInterchange->choice.notification;
			m_transferBatch = NULL;
			break;
		default:
			SetErrorAndLog(std::string("Для валидации был передан неподдерживаемый тип данных TAP: ") + 
				std::to_string(static_cast<long long>(dataInterchange->present)));
			m_validationResult = VALIDATION_IMPOSSIBLE;
	}
	if (!SetSenderNetworkID()) {
		SetErrorAndLog(std::string("Невозможно найти в базе сеть отправителя по TAP-коду. Проверьте корректность "
			"ее заведения. Файл не был загружен."));
		m_validationResult = VALIDATION_IMPOSSIBLE;
		return;
	}

	SetIOTValidationMode();
	IncomingTAPAllowed tapAllowed = IsIncomingTAPAllowed();
	switch (tapAllowed) {
	case INCOMING_TAP_ALLOWED:
		break;
	case INCOMING_TAP_NOT_ALLOWED:
		SetErrorAndLog(std::string("Валидация: Входящие TAP-файлы не разрешены для данной сети. Проверьте настройку поля "
			"\"Вид роуминга\" в справочнике \"Привязка к роуминговому координатору\"."));
		m_validationResult = VALIDATION_IMPOSSIBLE;
		return;
	case UNABLE_TO_DETERMINE:
	default:
		SetErrorAndLog(std::string("Валидация: Невозможно определить, разрешены или нет входящие TAP-файлы для данной сети."
			" Проверьте настройки справочника \"Привязка к роуминговому координатору\". Возможно, сеть привязана"
			" к одному координатору, а файлы поступают от другого."));
		m_validationResult = VALIDATION_IMPOSSIBLE;
		return;
	}
	if (m_transferBatch) {
		m_validationResult = ValidateTransferBatch();
	}
	else {
		m_validationResult = ValidateNotification();
	}
}


long TAPValidator::GetRapFileID() const
{
	return m_rapFile.GetID();
}


std::string TAPValidator::GetRapSequenceNum() const
{
	return m_rapFile.GetSequenceNumber();
}


long TAPValidator::GetSenderNetworkID() const
{
	if (m_mobileNetworkID > 0) {
		return m_mobileNetworkID;
	}
	else {
		otl_stream stream;
		long networkID;
		stream.open(1, "call BILLING.TAP3.GetUnrecognizedNetworkID() into :res /*long,out*/", m_otlConnect);
		stream >> networkID;
		stream.close();
		return networkID;
	}
}


long TAPValidator::GetIOTValidationMode() const
{
	return m_iotValidationMode;
}


TAPValidationResult TAPValidator::GetValidationResult() const
{
	return m_validationResult;
}

void TAPValidator::SetErrorAndLog(std::string& error)
{
	m_validationError = error;
	log(LOG_ERROR, error);
}

const std::string& TAPValidator::GetValidationError() const
{
	return m_validationError;
}
