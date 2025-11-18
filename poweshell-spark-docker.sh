# One-liner for PowerShell
$env:WORKSPACE_LOCATION = (Get-Location).Path
$env:SCRIPT_FILE_NAME = "rental-cars-agg1.py"
$docker_path = $env:WORKSPACE_LOCATION -replace '\\','/'

docker run -it -v "$env:USERPROFILE\.aws:/home/glue_user/.aws" -v "${docker_path}:/home/glue_user/workspace" -e AWS_PROFILE=default -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_spark_submit amazon/aws-glue-libs:glue_libs_4.0.0_image_01 spark-submit "/home/glue_user/workspace/$($env:SCRIPT_FILE_NAME)"
