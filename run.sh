hadoop fs -rm -r /user/xhermand/final_output
zip -d transpose.jar 'META-INF/*.SF' 'META-INF/*.RSA' 'META-INF/*SF'
hadoop jar transpose.jar Transpose /res/csv/example.csv final_output

