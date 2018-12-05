res=0
for i in {1..60}
do
   temp=$(go test | grep PASS | wc -l)
   echo $temp
   res=$((res+temp))
   echo $res
done
echo $res
