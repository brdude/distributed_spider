pipeline {
  agent any
  stages {
    stage('error') {
      steps {
        parallel(
          "Test 1": {
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