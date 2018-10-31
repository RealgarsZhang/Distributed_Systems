res=0
for i in {1..25}
do
   temp=$(go test | grep PASS | wc -l)
   echo $temp
   res=$((res+temp))
done
echo $res
