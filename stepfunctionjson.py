{
  "Comment": "Step Function to process multiple files using 2 lambdas with central fail handler",
  "StartAt": "LoadFilesFromS3",
  "States": {
    "LoadFilesFromS3": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:ap-south-1:640168441407:function:YC_beo_sfn_s3_file_read",
      "Parameters": {
        "s3Uri.$": "$.s3Uri"
      },
      "ResultPath": "$.fileData",
      "Next": "ProcessFiles"
    },
    "ProcessFiles": {
      "Type": "Map",
      "ItemsPath": "$.fileData.files",
      "MaxConcurrency": 5,
      "ResultPath": "$.results",
      "ItemProcessor": {
        "ProcessorConfig": {
          "Mode": "INLINE"
        },
        "StartAt": "RunFirstLambda",
        "States": {
          "RunFirstLambda": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:ap-south-1:640168441407:function:ycbeoocrlambda1",
            "Parameters": {
              "clusterId.$": "$.clusterId",
              "userId.$": "$.userId",
              "fileId.$": "$.fileId",
              "creditId.$": "$.creditId"
            },
            "ResultPath": "$.firstLambdaResult",
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "ResultPath": "$.error",
                "Next": "FailLambda"
              }
            ],
            "Next": "RunSecondLambda"
          },
          "RunSecondLambda": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:ap-south-1:640168441407:function:yc_beo_lambda2_structured",
            "Parameters": {
              "clusterId.$": "$.clusterId",
              "userId.$": "$.userId",
              "fileId.$": "$.fileId",
              "creditId.$": "$.creditId",
              "pages.$": "$.firstLambdaResult.pages",
              "text_content.$": "$.firstLambdaResult.text_content"
            },
            "ResultPath": "$.secondLambdaResult",
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "ResultPath": "$.error",
                "Next": "FailLambda"
              }
            ],
            "End": true
          },
          "FailLambda": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:ap-south-1:640168441407:function:yc_beo_fail_lambda",
            "Parameters": {
              "status": "failed",
              "error.$": "$.error",
              "clusterId.$": "$.clusterId",
              "userId.$": "$.userId",
              "fileId.$": "$.fileId",
              "creditId.$": "$.creditId"
            },
            "End": true
          }
        }
      },
      "End": true
    }
  }
}