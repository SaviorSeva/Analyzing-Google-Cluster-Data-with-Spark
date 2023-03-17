package navigation

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Navigation {

	def codept1(){
		// start spark with 1 worker thread
		val conf = new SparkConf().setAppName("Lab2").setMaster("local[1]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		// Read machine event file (whith empty values considered)
		val machine_events_whole_file = sc.textFile("../clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz")
		val machine_events_file = machine_events_whole_file.map(_.split(",", -1))
		machine_events_file.cache()

		/* Code fragment 001 - Count the number of machines */
		print("Total number of machines: ") 
		println(machine_events_file.map(x=>x(1)).distinct().count() + "\n")
		/* End of code fragment 001 */

		/* Code fragment 002 - Show the list of platforms */
		println("List of platforms: ") 
		machine_events_file.map(x=>x(3)).distinct().collect().foreach(println)
		/* End of code fragment 002 */

		/* Code fragment 003 - count the machine number of each platform */
		println("\nMachines in platform GTX : " + machine_events_file.filter(_.contains("GtXakjpd0CD41brK7k/27s3Eby3RpJKy7taB9S8UQRA=")).map(x=>x(1)).distinct().count())
		println("Machines in platform JQ1 : " + machine_events_file.filter(_.contains("JQ1tVQBMHBAIISU1gUNXk2powhYumYA+4cB3KzU29l8=")).map(x=>x(1)).distinct().count())
		println("Machines in platform HOF : " + machine_events_file.filter(_.contains("HofLGzk1Or/8Ildj2+Lqv0UGGvY82NLoni8+J/Yy0RU=")).map(x=>x(1)).distinct().count())
		println("Machines in platform 70Z : " + machine_events_file.filter(_.contains("70ZOvysYGtB6j9MUHMPzA2Iy7GRzWeJTdX0YCLRKGVg=")).map(x=>x(1)).distinct().count())
		/* End of code fragment 003 */

		/* Code fragment 004 - Verification of machines in JQ1 */
		println() 
		val platformJQ1 = machine_events_file.filter(_.contains("JQ1tVQBMHBAIISU1gUNXk2powhYumYA+4cB3KzU29l8="))
		val machines_in_JQ1 = platformJQ1.map(x=>x(1)).distinct().collect()

		val machines_in_other = machine_events_file
									.filter(!_.contains("JQ1tVQBMHBAIISU1gUNXk2powhYumYA+4cB3KzU29l8="))
									.map(x=>(x(1)))
									.distinct()
									.collect()

		machines_in_JQ1.foreach(x => if(machines_in_other.contains(x))
											{println("Machine " + x + " of JQ1 is in one of the other 3 platforms")}
											else{println("Machine " + x + " magically disappeared!!!")})
		/* End of code fragment 004 */

		// Code fragment 005 - Analysis on the platform JQ1
		print("In platform JQ1, each machine has x CPU ressources in y machine_events(x, y): ")
		val machines_CPU_in_JQ1 = platformJQ1.map(x=>(x(4), 1)).reduceByKey(_ + _).collect().foreach(println)
		/* End of code fragment 005 */

		// Code fragment 006 - Analysis on the platform GtX
		val platformGTX = machine_events_file.filter(_.contains("GtXakjpd0CD41brK7k/27s3Eby3RpJKy7taB9S8UQRA="))
		print("In platform GTX, each machine has x CPU ressources in y machine_events(x, y): ")
		platformGTX.filter(a => a(2) == "0").map(x=>(x(4), 1)).reduceByKey(_ + _).collect().foreach(println)
		/* End of code fragment 006 */

		// Code fragment 007 - Analysis on the platform HOF
		val platformHOF = machine_events_file.filter(_.contains("HofLGzk1Or/8Ildj2+Lqv0UGGvY82NLoni8+J/Yy0RU="))
		print("In platform HOF, each machine has x CPU ressources in y machine_events(x, y): ")
		platformHOF.filter(a => a(2) == "0").map(x=>(x(4), 1)).reduceByKey(_ + _).collect().foreach(println)
		/* End of code fragment 007 */

		// Code fragment 008 - Analysis on the platform Gt70ZX
		val platform70Z = machine_events_file.filter(_.contains("70ZOvysYGtB6j9MUHMPzA2Iy7GRzWeJTdX0YCLRKGVg="))
		print("In platform 70Z, each machine has x CPU ressources in y machine_events(x, y): ")
		platform70Z.filter(a => a(2) == "0").map(x=>(x(4), 1)).reduceByKey(_ + _).collect().foreach(println)
		/* End of code fragment 008 */
	}

	def codept2_1(){
		// start spark with 1 worker thread
		val conf = new SparkConf().setAppName("Lab2").setMaster("local[1]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		// Read machine event file (whith empty values considered) (without platform JQ1)
		val machine_events_whole_file = sc.textFile("../clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz")
		val machine_events_file = machine_events_whole_file.map(_.split(",", -1)).filter(!_.contains("JQ1tVQBMHBAIISU1gUNXk2powhYumYA+4cB3KzU29l8="))
		machine_events_file.cache()

		// Code Fragment 009 - Analysis of platform GTX
		val platformGTX = machine_events_file.filter(_.contains("GtXakjpd0CD41brK7k/27s3Eby3RpJKy7taB9S8UQRA="))
		
		// Get the change of compute resource at every timestamp.
		// The 1st element of GTXtemp is the timestamp in format long, 
		// and the 2nd is the change of compute power at that timestamp.
		// In general GTXtemp looks like [(0, +791.0), (1, -1.0), (2, +5.0), ...]
		val GTXtemp = platformGTX.filter(y=>y(2)!="2").map(x => {
																	if(x(2)=="1"){(x(0).toLong, ("-"+x(4)).toDouble)}
																	else{(x(0).toLong, x(4).toDouble)}
																}).reduceByKey(_ + _).sortBy(_._1)

		// Get the initialized compute power (which is the 1st element in GTXtemp). This value is 791.0.
		val initComputePower = GTXtemp.first()._2
		println("initComputePower of platform GTX (at timestamp 0) is " + initComputePower)
		var cpower = 0.0
		
		// Get the compute resource at every timestamp.
		// The 1st element of GTXtemp is the timestamp in format long, 
		// and the 2nd is the change of compute power at that timestamp.
		// In general GTXtemp2 looks like [(0, 791.0), (1, 790.0), (2, 795.0), ...]
		val GTXtemp2 = GTXtemp.map(x => {	
											cpower=cpower+x._2
											((x._1), cpower)	
		})

		/* 	
			I use the following line to show all the elements in GTXtemp2, I redirected the output to a file, 
			change it into .csv to create graphs. Since the result is quite long, I don't recommend decommenting 
			the line since it will write too many lines to the terminal. The output of the following line is 
			stored in the "codefrag009.txt" file.
		*/
		// GTXtemp2.collect().foreach(println)
		
		/* End of Code Fragment 009 - Analysis of platform GTX */

		// Code Fragment 010 - Avg, min, max compute power of platform GTX
		val GTXcp = GTXtemp2.map(_._2)
		println("Average compute power of platform GTX is " + GTXcp.mean)
		val minGTX = GTXcp.min()
		val maxGTX = GTXcp.max()
		println("Min compute power of platform GTX is " + minGTX)
		println("Max compute power of platform GTX is " + maxGTX)
		/* End of Code Fragment 010 */

		// Code Fragment 011 - Analysis of platform HOF
		val platformHOF = machine_events_file.filter(_.contains("HofLGzk1Or/8Ildj2+Lqv0UGGvY82NLoni8+J/Yy0RU="))
		
		// Get the change of compute resource at every timestamp of platform HOF
		val HOFtemp = platformHOF.filter(y=>y(2)!="2").map(x => {
																	if(x(2)=="1"){(x(0).toLong, ("-"+x(4)).toDouble)}
																	else{(x(0).toLong, x(4).toDouble)}
																}).reduceByKey(_ + _).sortBy(_._1)

		// Get the initialized compute power (which is the 1st element in HOFtemp). This value is 5781.5.
		val HOFinitComputePower = HOFtemp.first()._2
		println("platform HOF initComputePower (at timestamp 0) is " + HOFinitComputePower)
		var Hcpower = 0.0
		
		// Get the compute resource at every timestamp of platform HOF
		val HOFtemp2 = HOFtemp.map(x => {	
											Hcpower=Hcpower+x._2
											((x._1), Hcpower)	
		})
		// Again, the output for the following line is in codefrag011.txt
		// HOFtemp2.collect().foreach(println)
		
		/* End of Code Fragment 011 */

		// Code Fragment 012 - Avg, min, max compute power of platform HOF
		val HOFcp = HOFtemp2.map(_._2)
		println("Average compute power of platform HOF is " + HOFcp.mean)
		val minHOF = HOFcp.min()
		val maxHOF = HOFcp.max()
		println("Min compute power of platform HOF is " + minHOF)
		println("Max compute power of platform HOF is " + maxHOF)
		/* End of Code Fragment 012 */

		// Code Fragment 013 - Analysis of platform 70Z
		val platform70Z = machine_events_file.filter(_.contains("70ZOvysYGtB6j9MUHMPzA2Iy7GRzWeJTdX0YCLRKGVg="))
		
		// Get the change of compute resource at every timestamp of platform 70Z
		val s70Ztemp = platform70Z.filter(y=>y(2)!="2").map(x => {
																	if(x(2)=="1"){(x(0).toLong, ("-"+x(4)).toDouble)}
																	else{(x(0).toLong, x(4).toDouble)}
																}).reduceByKey(_ + _).sortBy(_._1)

		// Get the initialized compute power (which is the 1st element in 70Ztemp). This value is 30.75.
		val s70ZinitComputePower = s70Ztemp.first()._2
		println("platform 70Z initComputePower (at timestamp 0) is " + s70ZinitComputePower)
		var Scpower = 0.0
		
		// Get the compute resource at every timestamp of platform 70Z
		val s70Ztemp2 = s70Ztemp.map(x => {	
											Scpower=Scpower+x._2
											((x._1), Scpower)	
		})
		// Again, the output for the following line is in codefrag013.txt
		// s70Ztemp2.collect().foreach(println)

		/* End of Code Fragment 013*/

		// Code Fragment 014 - Avg, min, max compute power of platform 70Z
		val s70Zcp = s70Ztemp2.map(_._2)
		println("Average compute power of platform 70Z is " + s70Zcp.mean)
		val min70Z = s70Zcp.min()
		val max70Z = s70Zcp.max()
		println("Min compute power of platform 70Z is " + min70Z)
		println("Max compute power of platform 70Z is " + max70Z)
		/* End of Code Fragment 013*/
	}

	def codept2_2(){
		// start spark with 1 worker thread
		val conf = new SparkConf().setAppName("Lab2").setMaster("local[1]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		// Read machine event file (whith empty values considered) (without platform JQ1)
		val machine_events_whole_file = sc.textFile("../clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz")
		val machine_events_file = machine_events_whole_file.map(_.split(",", -1)).filter(!_.contains("JQ1tVQBMHBAIISU1gUNXk2powhYumYA+4cB3KzU29l8="))
		machine_events_file.cache()


		// Code Fragment 015 - Analysis of all platforms
		
		// Get the change of compute resource at every timestamp of all platforms
		val ALLtemp = machine_events_file.filter(y=>y(2)!="2").map(x => {
																	if(x(2)=="1"){(x(0).toLong, ("-"+x(4)).toDouble)}
																	else{(x(0).toLong, x(4).toDouble)}
																}).reduceByKey(_ + _).sortBy(_._1)

		// Get the initialized compute power (which is the 1st element in HOFtemp). This value is 791.0.
		val ALLinitComputePower = ALLtemp.first()._2
		println("initComputePower (at timestamp 0) is " + ALLinitComputePower)
		var Acpower = 0.0
		
		// Get the compute resource at every timestamp of platform HOF
		val ALLtemp2 = ALLtemp.map(x => {	
											Acpower=Acpower+x._2
											((x._1), Acpower)	
		})
		// Again, the output for the following line is in codefrag015.txt
		// ALLtemp2.collect().foreach(println)
		
		// End of Code Fragment 015 - Analysis of all platforms

		// Code Fragment 016 - Avg, min, max compute power of all platforms
		val ALLcp = ALLtemp2.map(_._2)
		println("Average compute power of all platforms is " + ALLcp.mean)
		val minALL = ALLcp.min()
		val maxALL = ALLcp.max()
		println("Min compute power of all platforms is " + minALL)
		println("Max compute power of all platforms is " + maxALL)
		/* End of Code Fragment 016 - Analysis of all platforms */
	}

	def codept3(){
		// start spark with 8 worker thread
		val conf = new SparkConf().setAppName("Lab2").setMaster("local[8]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		// Read machine event file (whith empty values considered)
		val task_events_whole_file = sc.textFile("../clusterdata-2011-2/task_events/*")
		val task_events_file = task_events_whole_file.map(_.split(",", -1))
		task_events_file.cache()

		// Code Fragment 017 - Generate (jobID, taskIndex, scheduling class) tuple & count them
		val tripleTuple = task_events_file.map(x=>(x(2), x(3), x(7)))
		val nb_jobs = task_events_file.map(x=>(x(2))).distinct().count()
		val nb_tasks = task_events_file.map(x=>(x(2), x(3))).distinct().count()
		val nb_tasks_scheduleclass = tripleTuple.distinct().count()
		// task_events_file.map(x=>(x(2), x(3))).collect().foreach(println)
		println("There is a total of " + nb_jobs + " jobs,")
		println(nb_tasks + " tasks and " + nb_tasks_scheduleclass + " task_schedule class.")
		/* End of Code Fragment 017 */

		// Code Fragment 018 - Get distribution of the number of jobs/tasks per scheduling class
		val getter = task_events_file
							.map(x=> ((x(2), x(3)), x(7)))
							.distinct()
							.map(y=> (y._2, 1))
							.reduceByKey(_ + _)
		getter.collect().foreach(println)
		/* End of Code Fragment 018 */
	}

	def codept4(){
		// start spark with 8 worker thread
		val conf = new SparkConf().setAppName("Lab2").setMaster("local[8]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		// ../clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz
		// Read machine event file (whith empty values considered)
		val task_events_whole_file = sc.textFile("../clusterdata-2011-2/task_events/*")
		val task_events_file = task_events_whole_file.map(_.split(",", -1))
		task_events_file.cache()

		// Code Fragment 019 - Get all EVICT task & all tasks according to their scheduling class 
		val evict_all = task_events_file
						.filter(x => x(5) == "2")
						.map(x=> ((x(5), x(7)), 1))
						.reduceByKey(_ + _)
						.sortBy(_._1)
		evict_all.collect().foreach(println)

		val sch_all = task_events_file
						.map(x=> ((x(7)), 1))
						.reduceByKey(_ + _)
						.sortBy(_._1)
		sch_all.collect().foreach(println)
		/* End of Code Fragment 019 */
	}
	
	def codept5(){
		// start spark with 8 worker thread
		val conf = new SparkConf().setAppName("Lab2").setMaster("local[8]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		// ../clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz
		// Read machine event file (whith empty values considered)
		val task_events_whole_file = sc.textFile("../clusterdata-2011-2/task_events/*")
		val task_events_file = task_events_whole_file.map(_.split(",", -1))
		task_events_file.cache()

		// Code Fragment 020 - Get all jobs and their machine IDs
		// job_machine_tuple (A, B) means there's B different machines being used for task A
		val job_machine_tuple = task_events_file
									.filter(x=> x(4) != "")
									.map(x=> (x(2), x(4)))
									.distinct()
									.map(_._1)
									.countByValue()
		// job_machine_tuple.take(10).foreach(println)
		println("Total:" + job_machine_tuple.size)
		println("TotalII:" + task_events_file.map(x=> x(2)).distinct().count())
		
									
		val one_machine_task = job_machine_tuple
								.filter(_._2 == 1)
								.size
		// one_machine_task.take(10).foreach(println)
		println("One:" + one_machine_task)

		val mul_machine_task = job_machine_tuple
								.filter(_._2 != 1)
								.size		
		println("Multi:" + mul_machine_task)

		/* Debug use */
		// println("Info on task 5379177169")
		// val task_info = task_events_file
		// 				.filter(x=> x(2) == "5379177169")
		// 				.map(x=> (x(0), x(2), x(3), x(4)))
		// task_info.collect().foreach(println)

		/* End of Code Fragment 020 */
	}

	def codept6_1(){
		// start spark with 8 worker thread
		val conf = new SparkConf().setAppName("Lab2").setMaster("local[8]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		// ../clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz
		// Read machine event file (whith empty values considered)
		val task_events_whole_file = sc.textFile("../clusterdata-2011-2/task_events/*")
		val task_events_file = task_events_whole_file.map(_.split(",", -1))
		task_events_file.cache()

		// Read resource usage file (whith empty values considered)
		val res_usage_whole_file = sc.textFile("../clusterdata-2011-2/task_usage/*")
		val res_usage_file = res_usage_whole_file.map(_.split(",", -1))
		res_usage_file.cache()

		// Code Fragment 021 - Get min max avg median CPU resource requests & CPU usage
		val cpu_request_table = task_events_file
								.filter(x=> x(9) != "")
								.map(x => x(9).toDouble)
								.distinct()
								.sortBy(x => x)
		val cpu_request_arrayList = cpu_request_table.collect()
		println("Min CPU resource request is " + cpu_request_arrayList.reduceLeft(_ min _))
		println("Max CPU resource request is " + cpu_request_arrayList.reduceLeft(_ max _))
		println("Avg CPU resource request is " + cpu_request_arrayList.reduceLeft(_ + _) / cpu_request_arrayList.length)
		println("Median CPU resource request is " + cpu_request_arrayList(cpu_request_arrayList.length/2))
		println("75% Median CPU resource request is " + cpu_request_arrayList(cpu_request_arrayList.length*3/4))
		/* End of Code Fragment 021 */

		// Code Fragment 022 - Get the min max avg CPU usage on each task
		val cpu_usage_table = res_usage_file
								.filter(x=> x(5) != "")
								.map(x => ((x(2), x(3)), (x(5).toDouble, 1)))
								.reduceByKey{
												case ((sumL, countL), (sumR, countR)) => 
												(sumL + sumR, countL + countR)
											}
								.mapValues{
											case (sum , count) => sum / count 
										  }
		println("Min CPU mean usage is " + cpu_usage_table.map(x=>x._2).reduce((acc,value) => { 
 						 														if(acc > value) value else acc}))
		println("Max CPU mean usage is " + cpu_usage_table.map(x=>x._2).reduce((acc,value) => { 
 						 														if(acc < value) value else acc}))
		println("Avg CPU mean usage is " + cpu_usage_table.map(x=>x._2).mean())
		/* End of Code Fragment 022 */


		// Code Fragment 023 - Get two list of tasks for CPU request above & below 0.06
		val more_resource_request = task_events_file
									.filter(x => x(9) != "")
									.map(x => ((x(2), x(3)), x(9).toDouble))
									.filter(_._2 >= 0.06)
									.map(a=>a._1)

		val less_resource_request = task_events_file
									.filter(x => x(9) != "")
									.map(x => ((x(2), x(3)), x(9).toDouble))
									.filter(_._2 < 0.06)
									.map(a=>a._1)

		println("More res task count : " + more_resource_request.count())
		println("Less res task count : " + less_resource_request.count())
		/* End of Code Fragment 023 */
		
		// Code Fragment 024 - Join procedure
		val joinnedMore = more_resource_request.map(x=> (x, 1))
						.join(cpu_usage_table)
						.map(y=>(y._1, y._2._2))
		val joinnedLess = less_resource_request.map(x=> (x, 1))
					.join(cpu_usage_table)
					.map(y=>(y._1, y._2._2))			
		println("Mean CPU usage rate for high res request tasks is " + joinnedMore.map(x=>x._2).mean())
		println("Mean CPU usage rate for low res request tasks is " + joinnedLess.map(x=>x._2).mean())
		/* End of Code Fragment 024 */
		
		/* Debug use */ 
		// Read resource usage file (whith empty values considered)
		// val res_usage_pt0_whole_file = sc.textFile("../clusterdata-2011-2/task_usage/part-00000-of-00500.csv.gz")
		// val res_usage_pt0_file = res_usage_pt0_whole_file.map(_.split(",", -1))
		// println("About job 3418309 task 0:")
		// val task_exemple_usage_percentage = res_usage_pt0_file
		// 								.filter(x => x(2) == "3418309")
		// 								.filter(x => x(3) == "0")
		// 								.map(x => ((x(2).toLong, x(3).toLong), (x(5).toDouble)))
		// task_exemple_usage_percentage.collect().foreach(println)
	}

	def codept7(){
		// start spark with 8 worker thread
		val conf = new SparkConf().setAppName("Lab2").setMaster("local[8]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		// ../clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz
		// Read machine event file (whith empty values considered)
		val task_events_whole_file = sc.textFile("../clusterdata-2011-2/task_events/*")
		val task_events_file = task_events_whole_file.map(_.split(",", -1))
		task_events_file.cache()

		// Read resource usage file (whith empty values considered)
		val res_usage_whole_file = sc.textFile("../clusterdata-2011-2/task_usage/*")
		val res_usage_file = res_usage_whole_file.map(_.split(",", -1))
		res_usage_file.cache()
		
		// Code Fragment 026 - Define HIGH RESOURCE CONSUMPTION
		val temp = res_usage_file.filter(x => x(5) != "")

		/* Enable this part to show how the value "27.5" is obtained. */
		// val high_res_threshold_500 = temp
		// 							.map(x => x(13).toDouble)
		// 							.sortBy(x => -x)
		// 							.take(500)
		// 							.reduceLeft(_ min _)
		// println("High resource consumption threshold defines as " + high_res_threshold_500 + ".")

		val high_res_threshold = 27.5
		/* End of Code Fragment 026 */

		// Code Fragment 027 - Get all EVICT task & their corresponding timestamps
		val evict_all = task_events_file
						.filter(x => x(5) == "2")
						.map(x=> (x(0).toLong, x(4).toLong))
		// evict_all.printSchema()
		/* End of Code Fragment 027 */

		// Code Fragment 028 - Get timestaps where high resource consumption is occured
		val high_res_comp = temp
							.map(x => (x(0).toLong, x(1).toLong, x(4).toLong, x(13).toDouble))
							.filter(_._4 > high_res_threshold)
							.collect()
		println("High res comp count: " + high_res_comp.length)
		/* End of Code Fragment 028 */

		// Code Fragment 029 - Count the number of high resource consumption in EVICT event.
		// WARNING: THIS IS EXTREMELY TIME CONSUMING (OVER 6 HOURS TO COMPLETE)
		// There might be a better way to significantly reduce the compute time (perhaps by join operation) though.
		var hit_count = 0
		high_res_comp.foreach(x => {
			if(!evict_all.filter(_._2 == x._3).filter(_._1 >= x._1).filter(_._1 <= x._2).isEmpty())
				hit_count = hit_count + 1
		})
		println("Hit count: " + hit_count)
		/* End of Code Fragment 029 */
	}

	def codept8(){
		// start spark with 8 worker thread
		val conf = new SparkConf().setAppName("Lab2").setMaster("local[8]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		// ../clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz
		// Read machine event file (whith empty values considered)
		val job_events_whole_file = sc.textFile("../clusterdata-2011-2/job_events/*")
		val job_events_file = job_events_whole_file.map(_.split(",", -1))
		job_events_file.cache()

		// println("A total of " + job_events_file.count() + "job events.")

		// Codefrag 030 - Get the number of SUBMIT event and SCHEDULE event
		val submit_job_time = job_events_file	
								.filter(x => x(3) == "0")
								.map(x => ((x(2), x(5)),x(0).toLong))

		val schedule_job_time = job_events_file	
								.filter(x => x(3) == "1")
								.map(x => ((x(2), x(5)), x(0).toLong))
								
		println("Submit count " + submit_job_time.count() + ".")
		println("\nSchedule count " + schedule_job_time.count() + ".")
		/* End of Code Fragment 030 */

		// Codefrag 031 - Get average schedule delay of jobs according to their schedule class
		val substracted = schedule_job_time
							.join(submit_job_time)
							.map(x => (x._1, x._2._1 - x._2._2))
		println("\nSubstracted count " + substracted.count())
		val avg_delay = substracted.filter(x => x._2 != 0).map(x => x._2).mean()
		println("\nAverage schedule delay: " + avg_delay)

		val zero_sch_class_delay = job_events_file
									.filter(x => x(3) == "1")
									.filter(x => x(5) == "0")
									.map(x => ((x(2), x(5)), x(0).toLong))
									.join(job_events_file
											.filter(x => x(3) == "0")
											.filter(x => x(5) == "0")
											.map(x => ((x(2), x(5)), x(0).toLong)))
									.map(x => (x._1, x._2._1 - x._2._2))
									.filter(x => x._2 != 0)		// This line filters out the tasks running at time 0. 
									.map(x => x._2).mean()
		println("\nSchedule class 0 average delay: " + zero_sch_class_delay)

		val one_sch_class_delay = job_events_file
									.filter(x => x(3) == "1")
									.filter(x => x(5) == "1")
									.map(x => ((x(2), x(5)), x(0).toLong))
									.join(job_events_file
											.filter(x => x(3) == "0")
											.filter(x => x(5) == "1")
											.map(x => ((x(2), x(5)), x(0).toLong)))
									.map(x => (x._1, x._2._1 - x._2._2))
									.filter(x => x._2 != 0)		// This line filters out the tasks running at time 0. 
									.map(x => x._2).mean()
		println("\nSchedule class 1 average delay: " + one_sch_class_delay)

		val two_sch_class_delay = job_events_file
									.filter(x => x(3) == "1")
									.filter(x => x(5) == "2")
									.map(x => ((x(2), x(5)), x(0).toLong))
									.join(job_events_file
											.filter(x => x(3) == "0")
											.filter(x => x(5) == "2")
											.map(x => ((x(2), x(5)), x(0).toLong)))
									.map(x => (x._1, x._2._1 - x._2._2))
									.filter(x => x._2 != 0)		// This line filters out the tasks running at time 0. 
									.map(x => x._2).mean()
		println("\nSchedule class 2 average delay: " + two_sch_class_delay)

		val three_sch_class_delay = job_events_file
									.filter(x => x(3) == "1")
									.filter(x => x(5) == "3")
									.map(x => ((x(2), x(5)), x(0).toLong))
									.join(job_events_file
											.filter(x => x(3) == "0")
											.filter(x => x(5) == "3")
											.map(x => ((x(2), x(5)), x(0).toLong)))
									.map(x => (x._1, x._2._1 - x._2._2))
									.filter(x => x._2 != 0)		// This line filters out the tasks running at time 0. 
									.map(x => x._2).mean()
		println("\nSchedule class 3 average delay: " + three_sch_class_delay)
		/* End of Code Fragment 031 */
	}

	def codept9(){
		// start spark with 8 worker thread
		val conf = new SparkConf().setAppName("Lab2").setMaster("local[8]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		// ../clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz
		// Read job event file (whith empty values considered)
		val job_events_whole_file = sc.textFile("../clusterdata-2011-2/job_events/*")
		val job_events_file = job_events_whole_file.map(_.split(",", -1))
		job_events_file.cache()
		
		// // Read task event file (whith empty values considered)
		// val task_events_whole_file = sc.textFile("../clusterdata-2011-2/task_events/*")
		// val task_events_file = task_events_whole_file.map(_.split(",", -1))
		// task_events_file.cache()

		// Codefrag 032 - Get the (jobID, execTime) tuple
		val temp0 = job_events_file
						.filter(x=>x(3) == "4")
						.map(x => (x(2), x(0).toLong))
						.join(job_events_file
									.filter(x=>x(3) == "1")
									.map(x => (x(2), x(0).toLong)))
						.map(x => (x._1, x._2._1 - x._2._2))

		println("\nJob average completion time: " + temp0.map(y => y._2).mean())
		/* End of Code Fragment 032 */
		
		// Codefrag 033 - Get the distribution of exe time for jobs.

		/* Grouped by execution time (sec) */
		// val distribution = temp0
		// 					.map(x=>(x._2/1000000, 1))
		// 					.reduceByKey(_ + _)
		// distribution.sortBy(_._1).collect().foreach(println)

		/* Grouped by log(executionTime) */
		val distributionLOG10 = temp0
							.map(x=>(java.lang.Math.log10(x._2/1000000).toInt, 1))
							.reduceByKey(_ + _)
		distributionLOG10.sortBy(_._1).collect().foreach(println)
	}

	def main(args: Array[String]): Unit = {
		
		/* For each question, un-comment the part for that question */
		
		// codept1()

		// codept2_1()
		// codept2_2()

		// codept3()

		// codept4()

		// codept5()

		// codept6_1()
		// codept6_2()

		// codept7()

		// codept8()

		// codept9()

		// prevent the program from terminating immediatly
		println("Press Enter to continue...")
		scala.io.StdIn.readLine()
	}
}

