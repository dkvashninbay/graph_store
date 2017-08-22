APP = 'app'
TESTS = 'tests'

namespace 'dev' do
    desc "Codestyle"
    task :lint do
        for item in [APP, 'service.py'] do
            sh "flake8 #{item}"
            sh "isort #{item} --diff"
        end
    end

    desc "Test"
    task :test do
        for item in [TESTS] do
            sh "pytest #{item}"
        end
    end
end
