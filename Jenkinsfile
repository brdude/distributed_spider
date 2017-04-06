pipeline {
  agent any
  stages {
    stage('error') {
      steps {
        parallel(
          "error": {
            sh 'echo "Hello World"'
            
          },
          "Test 2": {
            echo 'Well well well!'
            
          }
        )
      }
    }
  }
}