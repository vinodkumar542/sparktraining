
package com.dmac.analytics.spark;


/**
*
* 
* @author <a href="mailto:aravindh.chinnasamy@gmail.com">Aravindh Chinnasamy</a>
* @version %I%, %G%
* @since 1.0
*/
public final class SparkConnectionFactory {

	
	private static final SparkConnectionFactory INSTANCE = new SparkConnectionFactory().init();
	
	/**
	 * 
	 * @return
	 */
	public static SparkConnectionFactory getInstance() {
		
		return INSTANCE;
	}
	
	private SparkConnectionFactory init() 
	{
		return null;
	}
}
