# frozen_string_literal: true

set :application, 'libsys-airflow'
set :repo_url, 'https://github.com/sul-dlss/libsys-airflow.git'
set :user, 'libsys'

# Default branch is :master
ask :branch, `git rev-parse --abbrev-ref HEAD`.chomp

# Default value for :log_level is :debug
set :log_level, :info

# Default deploy_to directory is /var/www/my_app_name
set :deploy_to, "/home/libsys/#{fetch(:application)}"

# Default value for linked_dirs is []
# set :linked_dirs, %w[]
set :linked_dirs, %w[config]

# Default value for keep_releases is 5
set :keep_releases, 3

task :deploy do
  on roles(:app) do
    execute "source virtual-env/bin/activate"
    execute "cd #{release_path} && pip3 install -r requirements.txt"
    execute "cp #{release_path}/config/.env #{release_path}/."
  end
end

namespace :airflow do
  desc 'show running docker processes'
  task :ps do
    on roles(:app) do
      execute "cd #{release_path} && docker ps"
    end
  end

  desc 'run docker-compose build for airflow'
  task :build do
    on roles(:app) do
      execute "cd #{release_path} && docker-compose build"
    end
  end

  desc 'stop and remove all running docker containers'
  task :stop do
    on roles(:app) do
      execute "cd #{release_path}"
      execute "docker stop $(docker ps -a -q)"
      execute "docker rm $(docker ps -a -q)"
    end
  end

  desc 'run docker-compose build and initialize for airflow'
  task :start do
    on roles(:app) do
      execute "cd #{release_path}"
      execute "docker-compose build"
      execute "docker compose up airflow-init"
      execute "docker compose up -d"
    end
  end

  desc 'restart airflow'
  task :restart do
    on roles(:app) do
      stop
      start
    end
  end
end
