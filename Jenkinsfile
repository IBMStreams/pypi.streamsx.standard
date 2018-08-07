pipeline {
  agent { label 'streamsx_public' }
  stages {
    stage ('Python tests') {
      parallel {
    stage('Python 3.6 standalone') {
       steps {
         sh 'ci/test_python36_standalone.sh'
       }
    }
    stage('Python 3.5 standalone') {
       when { branch 'DISABLED' }
       steps {
         sh 'ci/test_python35_standalone.sh'
       }
    }
    stage('Python 2.7 standalone') {
       steps {
         sh 'ci/test_python27_standalone.sh'
       }
    }
      }
    }
  }
  post {
    always {
      junit "nose_runs/**/TEST-*.xml"
      publishHTML (target: [
          reportName: 'Coverage 27',
          reportDir: 'nose_runs/py27/coverage',
          reportFiles: 'index.html',
          keepAll: false,
          alwaysLinkToLastBuild: true,
          allowMissing: true
      ])
      publishHTML (target: [
          reportName: 'Coverage 36',
          reportDir: 'nose_runs/py36/coverage',
          reportFiles: 'index.html',
          keepAll: false,
          alwaysLinkToLastBuild: true,
          allowMissing: true
      ])
    }
  }
}
