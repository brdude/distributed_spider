pipeline {
  agent any
  stages {
    stage('Test Stage 1') {
      steps {
        parallel(
          "Test 1": {
            sh 'echo "Hello World"'
            
          },
          "Test 2": {
            sh 'echo "Well well well!"'
            
          }
        )
      }
    }
    stage('Test Stage 2') {
      steps {
        retry(count: 5) {
          echo 'hahahaha'
        }
        
      }
    }
    stage('Testing') {
      steps {
        timestamps() {
          isUnix()
          echo 'unix like!'
        }
        
      }
    }
  }
}
