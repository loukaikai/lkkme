package com.amarsoft.rwa.engine.me.calculation;


/**
 * 计算服务工厂类
 * <br>获取计算实现类
 * 
 * @author 陈庆
 * @version 1.0 2013-06-05
 *
 */
public class RWACalculationFactory {
	
	/**
	 * 私有化
	 */
	private RWACalculationFactory(){};

	/**
	 * 根据计算类型获取相应的计算实现类
	 * @param t 计算类型
	 * @return 返回计算实现类
	 */
	public static RWACalculation getService(RWACalculationType t) {
		RWACalculation calculation = null;
		switch (t) {
		case IRR:
			calculation = new IRRCalculation();
			break;
		
		case ER:
			calculation = new ERCalculation();
			break;
		
		case FER:
			calculation = new FERCalculation();
			break;
		
		case CR:
			calculation = new CRCalculation();
			break;
		
		case OR:
			calculation = new ORCalculation();
			break;
			
		default:
			break;
		}
		return calculation;
	}
	
}
