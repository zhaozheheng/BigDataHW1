I have bundle all the 4 questions into 1 jar file

For running my program, please using the following command:

hadoop jar <jar file direction> package_name.class_name <input direction> <output direction>
e.g. for my solutions:
    hadoop jar ~/Downloads/test/bigdata_hw1-1.0-SNAPSHOT.jar question_1.Question1 /user/zhaozheheng/input /user/zhaozheheng/out1
    hadoop jar ~/Downloads/test/bigdata_hw1-1.0-SNAPSHOT.jar question_2.Question2 /user/zhaozheheng/input /user/zhaozheheng/out2
    hadoop jar ~/Downloads/test/bigdata_hw1-1.0-SNAPSHOT.jar question_3.Question3 /user/zhaozheheng/input /user/zhaozheheng/out3
    hadoop jar ~/Downloads/test/bigdata_hw1-1.0-SNAPSHOT.jar question_4.Question4 /user/zhaozheheng/input /user/zhaozheheng/out4

All the results will be located in <output direction>/part-r-00000
