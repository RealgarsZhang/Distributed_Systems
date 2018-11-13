res=0
for i in {1..50}
do
   temp=$(go test | grep PASS | wc -l)
   echo $temp
   res=$((res+temp))
done
echo $res
