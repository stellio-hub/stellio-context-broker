#!/usr/bin/env bash
echo "Running detekt check..."
OUTPUT="/tmp/detekt-$(date +%s)"
COUNTER=0
EXIT_CODE=1




until [ $COUNTER -gt 5 ] || [ $EXIT_CODE -eq 0 ]
do
./gradlew detekt --auto-correct > $OUTPUT
EXIT_CODE=$?
echo "execute $COUNTER time"
cat $OUTPUT
COUNTER=$((COUNTER+1))
echo "detekt return $EXIT_CODE"
done
rm $OUTPUT

if [ $EXIT_CODE -eq 0 ] && [ $COUNTER -eq 1 ]; then
  echo "***********************************************"
  echo "             Validation succeeded              "
  echo "***********************************************"
  echo ""
  exit 0
fi

if [ $EXIT_CODE -eq 0 ] ; then
  echo "*************************************************"
  echo "                Validation failed                "
  echo "                  Fix succeeded                  "
  echo ""
  echo "         The fix were successfully applied       "
  echo "You can retry the commit with the applied changes"
  echo "*************************************************"
  echo ""
  exit 1
fi

if [ $EXIT_CODE -eq 1 ]; then
  echo "***********************************************"
  echo "                 detekt failed                 "
  echo "                unexpected error               "
  echo "***********************************************"
  echo ""
  exit 1
fi

if [ $EXIT_CODE -eq 2 ]; then
  echo "***********************************************"
  echo "                 detekt failed                 "
  echo " Please fix the above issues before committing "
  echo "***********************************************"
  exit 1
fi

if [ $EXIT_CODE -eq 2 ]; then
  echo "***********************************************"
  echo "                 detekt failed                 "
  echo "       Invalid detekt configuration file       "
  echo "***********************************************"
  echo ""
  exit 1
fi