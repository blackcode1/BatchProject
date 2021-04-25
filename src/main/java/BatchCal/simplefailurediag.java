package BatchCal;

import BatchDataPacket.BaseClassDataType.TaskState;
import cn.edu.thss.rcsdk.RealTimeAlg;
import com.alibaba.fastjson.JSONObject;
import thss.rcsdk.DeviceTimeWindow;
import ty.pub.RawDataPacket;
import ty.pub.TransPacket;

import java.util.*;


public class simplefailurediag extends RealTimeAlg<JSONObject> {
	/*
	@param rawInput: RawDataPacket类型流输入数据包
	@param transInput: TransPacket类型流输入数据包
	@param jsonInput: JSONObject类型流输入数据包
	@param condition: 工况ID列表
	@param config: 批数据集输入。Map的Key是数据集UID，Value是数据集UID对应的表，每一行记录以Map<String,String>类型保存。
	@param publicState: 算法依赖的公有状态集合（由其它任务产生的状态数据）
	@param privateState: 私有状态（本任务产生的状态数据）
	@param return: List<JSONObject>
	*/
	DeviceTimeWindow timeWindow = new DeviceTimeWindow(30000L, 5000L);

	@Override
	public Boolean init(RawDataPacket rawDataPacket, TransPacket transPacket, JSONObject jsonObject, List<String> list, Map<String, List<Map<String, String>>> map, List<TaskState> list1, TaskState taskState) throws Exception {
		//填写用户算法初始化逻辑
		return true;
	}
	/*
	故障状态：0正常，1异常
	故障起始时间-数据包时间、广播时间
	若数据包中压力正常：故障状态=0，时间=0
	如异常：故障状态=1
	*/

	@Override
	protected List<JSONObject> calc(RawDataPacket rawDataPacket, TransPacket transPacket, JSONObject jsonObject, List<String> list, Map<String, List<Map<String, String>>> map, List<TaskState> list1, TaskState taskState) throws Exception {
		//输出的结果res
		List<JSONObject> res = new ArrayList<>();

		List<JSONObject> inputList = timeWindow.myTimeWindow(jsonObject);
		if(inputList.size() != 0){
			//填写用户算法计算逻辑
		}

		//判断是否获取到故障模型信息
		if(map == null || !map.containsKey("BC75142E7C7A344AAA84B8D1B7E36214") || map.get("BC75142E7C7A344AAA84B8D1B7E36214").size() == 0){
			return res;
		}
		Long systemTime = new Date().getTime();

		//故障模型信息 BC75142E7C7A344AAA84B8D1B7E36214是故障模型信息所在的关系数据集的全局唯一标识
		Long duration = Long.valueOf(map.get("BC75142E7C7A344AAA84B8D1B7E36214").get(0).get("duration"));//持续时间
		Integer threshold = Integer.valueOf(map.get("BC75142E7C7A344AAA84B8D1B7E36214").get(0).get("threshold"));//阈值
		String failureInfo = map.get("BC75142E7C7A344AAA84B8D1B7E36214").get(0).get("failureinfo");//报警信息
		Boolean ebro = (map.get("BC75142E7C7A344AAA84B8D1B7E36214").get(0).get("ebro").equals("t"));//是否紧急状态

		//判断是否是广播数据包触发的计算，广播数据包时输入为null，广播数据包有实时计算系统每s生成一包触发计算
		if(jsonObject != null){
			Map<String, String> baseInfoMap = (Map<String, String>) jsonObject.get("baseInfoMap");//从输入数据包中获取线路、车辆等基础信息
			Map<String, Map<String, String>> workStatusMap = (Map<String, Map<String, String>>) jsonObject.get("workStatusMap");//获取工况信息
			String deviceID = baseInfoMap.get("TP_LineNumber").substring(0,4)+ "00" + "." + baseInfoMap.get("TrainrNumber") + "." + baseInfoMap.get("TP_HeadOrTail");//生成车辆信息
			//初始化状态【车辆信息】 taskState:List,Map<String, List>
			List<String> carState = taskState.getListState();
			if(carState.size() == 0){
				carState.add(deviceID);
			}
			else {
				carState.set(0, deviceID);
			}
			taskState.setListState(carState);

			Double mainPacketTime = Double.valueOf(baseInfoMap.get("TP_Timestamp"));//大包时间戳,ms

			//map<车厢.工况，ma<时间戳增量，工况值>>>变为map<车厢，map<工况，工况值列表>>
			Map<String, Map<String, List<Double>>> conditionMap = new HashMap<>();//车厢。工况。工况值列表
			for(Map.Entry<String, Map<String, String>> entry: workStatusMap.entrySet()){
				String carriage = entry.getKey().split("\\.")[0];//车厢
				String conditionID = entry.getKey().split("\\.")[1];//工况
				List<Double> value = new ArrayList<>();//工况值列表，其中包含10个工况，工况的时间戳增量分别为0,102,205,307……
				for(int i = 0; i < 10; i++){
					String pos = String.valueOf(new Double(i * 102.4).intValue());//时间戳增量
					if(entry.getValue().containsKey(pos)){
						value.add(Double.valueOf(entry.getValue().get(pos)));
					}
					else {
						value.add(null);
					}
				}
				if(! conditionMap.containsKey(carriage)){
					conditionMap.put(carriage, new HashMap<>());
				}
				conditionMap.get(carriage).put(conditionID, value);
			}
			//每个车厢分别计算故障
			for(Map.Entry<String, Map<String, List<Double>>> entry: conditionMap.entrySet()){
				String carriage = entry.getKey();

				//初始化Map状态，Map<车厢，[故障等级，故障发生的工况时间，故障发生的系统时间]>
				List<String> carriageState = new ArrayList<>();
				carriageState.add("0");
				carriageState.add("0");
				carriageState.add("0");
				if(taskState.getMapState().containsKey(carriage)){
					carriageState = taskState.getMapState().get(carriage);
				}

				//一个大包中有10个工况，按时间顺序计算
				for(int i = 0; i < 10; i++){
					String failureLv = carriageState.get(0);//故障等级
					if(entry.getValue().get("BI11Value_IB02A").get(i) == null
							|| entry.getValue().get("CvPressureSensorLBtx").get(i) == null
							|| entry.getValue().get("BC_PressureBtx").get(i) == null
							|| entry.getValue().get("BC_P_TargetBtx").get(i) == null){
						continue;//故障工况数据不全
					}
					Boolean EB_RO = (entry.getValue().get("BI11Value_IB02A").get(i) != 0);//紧急制动信号
					Double CvPressureSensorLBtx = entry.getValue().get("CvPressureSensorLBtx").get(i);//预控压力信号
					Double BC_PressureBtx = entry.getValue().get("BC_PressureBtx").get(i);//制动缸压力信号
					Double BC_P_TargetBtx = entry.getValue().get("BC_P_TargetBtx").get(i);//制动缸压力目标值
					Double packetTime = mainPacketTime + i * 100;//工况时间

					if(EB_RO == ebro){
						if(CvPressureSensorLBtx < BC_P_TargetBtx - threshold && BC_PressureBtx < BC_P_TargetBtx - threshold){//满足故障条件
							if(failureLv == "0"){//故障等级为0，表示正常，此时由正常转向故障，表示故障发生
								carriageState.set(0, "1");
								carriageState.set(1, String.valueOf(packetTime));
								carriageState.set(2, String.valueOf(systemTime));
								taskState.updateMapState(carriage, carriageState);//将故障等级和故障发生时间存入状态
							}
							else if(failureLv == "1"){//故障等级为1，表示已经故障，此时需判断是否进行报警
								Double startPacketTime = Double.valueOf(carriageState.get(1));//故障发生的工况时间
								Long startSystemTime = Long.valueOf(carriageState.get(2));//故障发生的系统时间
								if(packetTime - startPacketTime > duration || systemTime - startSystemTime > duration){//按工况数据或者系统时间计算，达到故障持续时间
									carriageState.set(0, "2");
									taskState.updateMapState(carriage, carriageState);//故障等级升至2，存入状态
									JSONObject resJson = new JSONObject();
									resJson.put("failureInfo", failureInfo + ", start at ("+startPacketTime + ","+startSystemTime+"), find at ("+packetTime + ","+systemTime+")");
									resJson.put("table", "root." + deviceID);
									resJson.put("carriage", "Carriage"+carriage);
									resJson.put("time", startPacketTime);
									res.add(resJson);//输出报警信息
								}
							}
							JSONObject resJson = new JSONObject();
							resJson.put("failureInfo", "error at "+ packetTime + ",CvPressureSensorLBtx:"+CvPressureSensorLBtx+",BC_PressureBtx:"+BC_PressureBtx+",BC_P_TargetBtx:"+BC_P_TargetBtx);
							resJson.put("table", "root." + deviceID);
							resJson.put("carriage", "Carriage"+carriage);
							resJson.put("time", packetTime);
							res.add(resJson);//输出故障信息
						}
						else {//未满足故障条件，即正常
							carriageState.set(0, "0");
							taskState.updateMapState(carriage, carriageState);//故障等级变为0，存入状态
							JSONObject resJson = new JSONObject();
							resJson.put("failureInfo", "normal at "+packetTime+ ",CvPressureSensorLBtx:"+CvPressureSensorLBtx+",BC_PressureBtx:"+BC_PressureBtx+",BC_P_TargetBtx:"+BC_P_TargetBtx);
							resJson.put("table", "root." + deviceID);
							resJson.put("carriage", "Carriage"+carriage);
							resJson.put("time", packetTime);
							res.add(resJson);//输出故障信息
						}
					}
				}

			}
		}
		else {//由广播数据包触发计算，此时对于正常的设备和已报警的设备，无需处理，仅判断故障中的设备是否需要报警
			String deviceID = null;//从状态中获取设备信息，由于输出报警信息
			if(taskState.getListState().size() > 0){
				deviceID = taskState.getListState().get(0);
			}
			//遍历设备下所有车厢
			for(Map.Entry<String, List<String>> entry: taskState.getMapState().entrySet()){
				String carriage = entry.getKey();
				List<String> carriageState = entry.getValue();
				String failureLv = carriageState.get(0);
				Double startPacketTime = Double.valueOf(carriageState.get(1));
				Long startSystemTime = Long.valueOf(carriageState.get(2));
				if(failureLv == "1" && systemTime - startSystemTime > duration){//故障等级为1，表示已经故障，此时需判断是否进行报警，并且按系统时间计算，达到故障持续时间
					carriageState.set(0, "2");//故障等级升至2，存入状态
					taskState.updateMapState(carriage, carriageState);
					JSONObject resJson = new JSONObject();
					resJson.put("failureInfo", failureInfo + ", start at ("+startPacketTime + ","+startSystemTime+"), find at (null,"+systemTime+"), duration:"+duration);
					resJson.put("table", "root." + deviceID);
					resJson.put("carriage", "Carriage"+carriage);
					resJson.put("time", startPacketTime);
					res.add(resJson);//输出报警信息
				}
			}
		}

		return res;//输出故障诊断结果
	}

}
