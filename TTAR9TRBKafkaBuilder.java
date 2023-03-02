//------------------------------------------------------------------------------------------------
//Name:    AR9TRBKafkaBuilder.java
//Purpose: This class is called by  AR9TRBTOKAFKA daemon. It push the data from 
//         TRB1_SUB_LOG table to KAFKA topic for specific sub_appl_id and actv_Code_id.
//Products: XACCT, Actix International, jNetX
//------------------------------------------------------------------------------------------------
package amdocs.ar.builder.kafka;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import amdocs.acmarch.core.ApplicationContext;
import amdocs.acmarch.core.ApplicationObject;
import amdocs.acmarch.exceptions.ACMException;
import amdocs.acmarch.utils.log.CustomizedLogger;
import amdocs.ar.datalayer.datatypes.AR9TrbDataReaderDt;
import amdocs.ar.datalayer.dvm.AR9ViewFactory;
import amdocs.ar.datalayer.dvm.viewdata.AR9SelectTRBDataDBViewData;
import amdocs.ar.datalayer.dvm.views.AR9SelectTRBDataCursor;
import amdocs.ar.datalayer.internaldatatypes.AR9TrbDataReaderDtExt;
import amdocs.ar.general.batchframework.BatchBuilder;
import amdocs.ar.general.batchframework.Director;
import amdocs.ar.general.error.ARErrorMessages;
import amdocs.ar.general.error.ApplicationException;
import amdocs.ar.general.server.ARApplicationContext;
import amdocs.ar.general.server.AREnvironment;
import amdocs.ar.general.util.AR9Constants;
import amdocs.ar.general.util.AR9JSONConverterUtils;
import amdocs.ar.general.util.AR9Utilities;
import amdocs.ar.utils.LoggerUtils;

import com.fasterxml.jackson.databind.JsonNode;

public class AR9TRBKafkaBuilder implements BatchBuilder, ApplicationObject
{

	private ARApplicationContext appContext;
	private CustomizedLogger logger;
	
	private ArrayList<AR9TrbDataReaderDt> updateTRBDataList; //NOSONAR
	private AR9SelectTRBDataDBViewData[] trbDataDBViewData = null;
	private AR9JSONConverterUtils converterUtil=null;
	private AR9SelectTRBDataCursor selectTRBDataCursor = null;
	private boolean readNextPage = false;
	private Producer<String, JsonNode> producer = null;
	private AR9Utilities ar9Utilities = null;
	private boolean executeProcess = false;
	private int trbDataCounter;
	private String processId = null;
	private int topicCount = 0;
	private String firstTopic = null;
	private String secongTopic = null;
	private String thirdTopic = null;
	private String fourthTopic = null;
	private String fifthTopic = null;
	private int trbActvCodeId;
	private int trbSubApplId;
	private int cursorSize = 0;
	private int commitSize = 0;
	

	public AR9TRBKafkaBuilder(ARApplicationContext arApplicationContext) throws ACMException
	{
		appContext = arApplicationContext;
		logger = appContext.logger(LoggerUtils.BATCH_LOGGER);
		logger.enter();
		logger.exit();
	}

	@Override
	public ApplicationContext getApplicationContext()
	{
		return appContext;
	}

	@Override
	public void init(Director paramDirector, String[] paramArrayOfString) throws ACMException
	{
		logger.enter();
		
		try
		{
			cursorSize = Integer.parseInt(AREnvironment.getInstance().getEnvironmentProperty(AR9Constants.AR_KAFKA_TRB_DATA_CURSOR_SIZE));
			trbActvCodeId = Integer.parseInt(AREnvironment.getInstance().getEnvironmentProperty(AR9Constants.AR_KAFKA_TRB_ACTV_CODE_ID));
			trbSubApplId = Integer.parseInt(AREnvironment.getInstance().getEnvironmentProperty(AR9Constants.AR_KAFKA_TRB_SUB_APPL_ID));
			commitSize = Integer.parseInt(AREnvironment.getInstance().getEnvironmentProperty(AR9Constants.AR_KAFKA_UPDATE_COMMIT_SIZE));
			processId = System.getProperty("program.name");
			topicCount = Integer.parseInt(appContext.getEnvironment().getEnvironmentProperty(AR9Constants.AR_KAFKA_TOPICS_COUNT));
			firstTopic = appContext.getEnvironment().getEnvironmentProperty(AR9Constants.AR_KAFKA_FIRST_TOPIC);
			secongTopic = appContext.getEnvironment().getEnvironmentProperty(AR9Constants.AR_KAFKA_SECOND_TOPIC);
			thirdTopic = appContext.getEnvironment().getEnvironmentProperty(AR9Constants.AR_KAFKA_THIRD_TOPIC);
			fourthTopic = appContext.getEnvironment().getEnvironmentProperty(AR9Constants.AR_KAFKA_FOURTH_TOPIC);
			fifthTopic = appContext.getEnvironment().getEnvironmentProperty(AR9Constants.AR_KAFKA_FIFTH_TOPIC);
			
			logger.debug("Select data cursor size is: "+cursorSize);
			logger.debug("Update data commit size is: "+commitSize);
			logger.debug("ProcessId is: "+processId);
			logger.debug("topicCount is: "+topicCount);
			logger.debug("firstTopic is: "+firstTopic);
			
			ar9Utilities = new AR9Utilities(appContext);
			Properties props = new Properties();
			
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appContext.getEnvironment().getEnvironmentProperty(AR9Constants.AR_KAFKA_BOOTSTRAP_SERVERS));
	       	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	      	props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
	      	
	      	if(AR9Constants.YES_NO_INDICATOR_TRUE.equals(appContext.getEnvironment().getEnvironmentProperty(AR9Constants.ENABLE_SSL_CONNECTION)))
	      	{
	      		logger.debug("Setting SSL properties.");
	      		
		      	props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, appContext.getEnvironment().getEnvironmentProperty(AR9Constants.SSL_SECURITY_PROTOCOL));
		        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, appContext.getEnvironment().getEnvironmentProperty(AR9Constants.SSL_TRUSTSTORE_LOCATION_CONFIG));
		        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  appContext.getEnvironment().getEnvironmentProperty(AR9Constants.SSL_TRUSTSTORE_PASSWORD_CONFIG));
		        props.put(SaslConfigs.SASL_JAAS_CONFIG,appContext.getEnvironment().getEnvironmentProperty(AR9Constants.AR_KAFKA_JASS_CONFIG));
		        props.put(SaslConfigs.SASL_MECHANISM,appContext.getEnvironment().getEnvironmentProperty(AR9Constants.AR_KAFKA_SASL_MECHANISM));
	      	}
	      
			producer = new KafkaProducer<String, JsonNode>(props);
			updateTRBDataList = new ArrayList<AR9TrbDataReaderDt>();
			converterUtil = new AR9JSONConverterUtils(appContext);
		
			getTRBSubLogData();
		}
		catch(Exception e) //NOSONAR
		{
			logger.error("Exception occured while fetching initial details....",e);
             
			if(!ar9Utilities.isDaemonAlreadyDown(processId))
         	{
				ar9Utilities.updateShutDownFlag(processId);
         	}
			
			throw new ApplicationException(e);
		}
		logger.exit();
	}

	

	@Override
	public BusinessEntity next() throws ACMException
	{
		logger.enter();
		
		AR9TRBDataHolder trbDataHolder = null;
		
		try
		{
			if(readNextPage)
			{
				logger.debug("In readNextPage..");
				trbDataDBViewData =  selectTRBDataCursor.nextPage();
				
				if(trbDataDBViewData == null || trbDataDBViewData.length < 1)
				{
					logger.debug("trbDataDBViewData has no data........");
					executeProcess = true;
					return null;
				}
				
				trbDataCounter = 0;
			}
			
			if(trbDataDBViewData != null &&  trbDataDBViewData.length > trbDataCounter )
			{
				logger.debug("Record counter is: "+(trbDataCounter+1));
				trbDataHolder = new AR9TRBDataHolder(trbDataDBViewData[trbDataCounter].getAR9TrbDataReaderDtExt());
				trbDataCounter++;
				readNextPage = false;
			}
			else
			{
				trbDataDBViewData = null;
				readNextPage = true;
				logger.debug("All data read done for current cursor...");
			}
		}
		catch (SQLException e)
		{
			logger.error(e);
			
			if(!ar9Utilities.isDaemonAlreadyDown(processId))
         	{
				ar9Utilities.updateShutDownFlag(processId);
         	}
			
			throw new ApplicationException(ARErrorMessages.BATCH_FRAMEWORK__FAIL_TO_RETIEVE_NEXT_RECORD, e);
		}
		logger.exit();
		return trbDataHolder;
	}

	@Override
	public void run(ArrayList trbDataHolderList, ARApplicationContext applicationContext) throws ACMException
	{
		logger.enter();
		
		logger.debug("trbDataHolderList size: "+trbDataHolderList.size());
		
		try
		{
			JsonNode json = null;
			
			for (int i = 0; i < trbDataHolderList.size(); i++)
			{
				AR9TrbDataReaderDt trbDataDT = ((AR9TRBDataHolder) trbDataHolderList.get(i)).getTRBDataDt();
				
				if (trbDataDT != null)
				{
					logger.debug("trbDataDT is: "+trbDataDT);
					
					json = converterUtil.converttoJson(trbDataDT.getGeneralData());
					
					ProducerRecord<String, JsonNode> producerRecord = selectKakfaTopic(json, i);
		            
			    	producer.send(producerRecord).get();
		                  
		            synchronized (this)
					{
		            	logger.debug("Adding successful records in arraylist for updating the trb trx status to 'PR' ");
						
						if (commitSize > updateTRBDataList.size())
						{
							updateTRBDataList.add(trbDataDT);
						}
						else
						{
							updateTRBDataList.add(trbDataDT);
							updateTRBStatus();
						}
					 }
	              }  
			  }
		 }
		 catch (Exception e) 
         {  
             logger.error("Exception occured while calling kafka....",e);
             
             synchronized (this) 
             {
	           	  if(!ar9Utilities.isDaemonAlreadyDown(processId))
	           	  {
	           		  ar9Utilities.updateShutDownFlag(processId);
	           	  }
             }
         } 

		logger.exit();
	}

	private ProducerRecord<String, JsonNode> selectKakfaTopic(JsonNode json,int i) 
	{
		logger.enter();
		
		ProducerRecord<String, JsonNode> producerRecord = null;
		
		switch(i % topicCount)
		{
			case 0: producerRecord = new ProducerRecord<String, JsonNode>(firstTopic , json);
					break;
			case 1: producerRecord = new ProducerRecord<String, JsonNode>(secongTopic , json);
					break;
			case 2: producerRecord = new ProducerRecord<String, JsonNode>(thirdTopic , json);
					break;
			case 3: producerRecord = new ProducerRecord<String, JsonNode>(fourthTopic , json);
					break;
			case 4: producerRecord = new ProducerRecord<String, JsonNode>(fifthTopic , json);
					break;
			default: producerRecord = new ProducerRecord<String, JsonNode>(firstTopic , json);
		}
		logger.exit();
		return producerRecord;
	}
	
	private void getTRBSubLogData() throws ACMException
	{
		logger.enter();

		try
		{
			AR9SelectTRBDataDBView selectTRBDataDBView = AR9ViewFactory.getInstance().getAR9SelectTRBDataDBView();

			selectTRBDataCursor = selectTRBDataDBView.selectTRBSubLogData(appContext, null, trbSubApplId, trbActvCodeId);
			
			if (selectTRBDataCursor != null)
			{
				logger.debug("Data found from TR1_SUB_LOG table....");
				selectTRBDataCursor.setPageSize(cursorSize);
				readNextPage = true;
			}
		}
		catch(SQLException e)
		{
			logger.error("SQL exception occured when fetching data from TR1_SUB_LOG table....");
			throw new ApplicationException(e);
		}

		logger.exit();
	}
	
	private void updateTRBStatus() throws ACMException
	{
		logger.enter();
		
		if (updateTRBDataList != null && updateTRBDataList.size() > 0)
		{
			int listSize = updateTRBDataList.size();
			
			logger.debug("Final trb data list has size" + listSize);
			
			appContext.getTransactionHelper().createMinorTransaction(true);
			try
			{
				AR9SelectTRBDataDBView trbDataDBView = AR9ViewFactory.getInstance().getAR9SelectTRBDataDBView();
				AR9SelectTRBDataDBViewData[] updateTRBdatas = new AR9SelectTRBDataDBViewData[listSize];
				long[] subTrxIds = new long[listSize];
				int[] subAppIds= new int[listSize];
				int[] actvCodeIds = new int[listSize];
				
				for (int i = 0; i < listSize; i++)
				{
					updateTRBdatas[i] = new AR9SelectTRBDataDBViewData();
					AR9TrbDataReaderDtExt ar9TrbDataReaderDtExt = (AR9TrbDataReaderDtExt)updateTRBDataList.get(i);
					ar9TrbDataReaderDtExt.setTrxStatus("PR");
					
					subTrxIds[i] = ar9TrbDataReaderDtExt.getSubTrxId();
					subAppIds[i] = ar9TrbDataReaderDtExt.getSubApplId();
					actvCodeIds[i] = ar9TrbDataReaderDtExt.getActvCodeId();
					
					updateTRBdatas[i].setAR9TrbDataReaderDtExt(ar9TrbDataReaderDtExt);
				}
				
				trbDataDBView.updateTrb1SubLog(appContext, null, updateTRBdatas, subTrxIds, subAppIds, actvCodeIds);
				
				logger.debug("commiting transaction");
				appContext.getTransactionHelper().getCurrentMinorTransaction().commit();
				
				updateTRBDataList.clear();

			}
			catch (SQLException e)
			{
				logger.error(e);
				logger.error("Exception thrown while update TRB1_SUB_LOG table .....Going to perform rollback");
				appContext.getTransactionHelper().getCurrentMinorTransaction().rollback();
				throw new ApplicationException(e);
			}
		}
		logger.exit();
	}

	public void performFinalActivities() throws ACMException
	{
		logger.enter();
		
		updateTRBStatus();
		
		logger.debug("Closing kafka producer....");
    	 
    	if(producer != null)
        {
			producer.flush();
        	producer.close();
        }
		
		logger.exit();
	}

	@Override
	public String getBatchApplicationId()
	{
		return AR9Constants.AR_KAFKA_BATCH_APPL_ID;
	}

	@Override
	public void setMappers(Director arg0, String[] arg1) throws ACMException
	{

	}

}
