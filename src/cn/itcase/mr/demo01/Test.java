package cn.itcase.mr.demo01;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String str = "!!！？？!!!!%*）%￥！KTV去符号标号！！当然,，。!!..**半角";
		
		str = str.replaceAll("[\\pP\\p{Punct}]", "");  
		System.out.println(str);
	}

}
