package com.amarsoft.rwa.engine.me.step;

/**
 * 步骤工厂类
 * <br>获取步骤实现类
 * 
 * @author 陈庆
 * @version 1.0 2013-06-05
 *
 */
public class StepFactory {
	
	/**
	 * 步骤工厂类构造方法私有化
	 */
	private StepFactory() {
		
	}

	/**
	 * 根据步骤类型返回相应的步骤实现类
	 * @param t 步骤类型
	 * @return 步骤实现类
	 */
	public static Step getStep(StepType t) {
		Step step = null;
		switch (t) {
		case LOADDATA:
			step = new LoadDataStep(t);
			break;
			
		case MAPPINGPARAMS:
			step = new MappingParamsStep(t);
			break;
		
		case CALCULATERWA:
			step = new CalculateRWAStep(t);
			break;
		
		case INSERTRESULT:
			step = new InsertResultStep(t);
			break;
		
		default:
			break;
		}
		return step;
	}
	
}
