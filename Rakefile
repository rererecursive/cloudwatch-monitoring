require 'cfndsl/rake_task'
require 'rake'
require 'tempfile'
require 'yaml'
require 'json'
require_relative 'ext/common_helper'
require_relative 'ext/alarms'
require 'fileutils'
require 'digest'
require 'aws-sdk'


namespace :cfn do

  desc('Generate CloudFormation for CloudWatch alarms')
  task :generate do

    ARGV.each { |a| task a.to_sym do ; end }

    output_stream = nil
    if ARGV[1] == "--verbose"
      output_stream = STDOUT if ARGV.member? "--verbose"
      ARGV.delete_at(1)
    end
    customer = ARGV[1]
    application = ARGV[2]

    if !customer
      puts "Usage:"
      puts "rake cfn:generate <customer> [application]"
      exit 1
    end

    config = load_config(customer, application)
    customer_alarms_config = config['alarms']
    templates = config['templates']

    # Create an array of alarms based on the templates associated with each resource
    alarms = []
    resources = customer_alarms_config['resources']
    metrics = customer_alarms_config['metrics']
    hosts = customer_alarms_config['hosts']
    hosts ||= {}
    endpoints = customer_alarms_config['endpoints']
    endpoints ||= {}
    rme = { resources: resources, metrics: metrics, endpoints: endpoints, hosts: hosts }
    source_bucket = customer_alarms_config['source_bucket']
    output_path = config['output_path']
    alarms_file = config['alarms_file']
    upload_path = config['upload_path']

    rme.each do | k,v |
      if !v.nil?
        v.each do | resource,attributes |
          # set environments to 'all' by default
          environments = ['all']
          # Support config hashs for additional parameters
          params = {}
          if attributes.kind_of?(Hash)
            attributes.each do | a,b |
              environments = b if a == 'environments'
              # Convert strings to arrays for consistency
              if !environments.kind_of?(Array) then environments = environments.split end
              params[a] = b if !['template','environments'].member? a
            end
            templatesEnabled = attributes['template']
          else
            templatesEnabled = attributes
          end
          # Convert strings to arrays for looping
          if !templatesEnabled.kind_of?(Array) then templatesEnabled = templatesEnabled.split end
          templatesEnabled.each do | templateEnabled |
            if !templates['templates'][templateEnabled].nil?
              # If a template is provided, inherit that template
              if !templates['templates'][templateEnabled]['template'].nil?
                template_from = Marshal.load( Marshal.dump(templates['templates'][templates['templates'][templateEnabled]['template']]) )
                template_to = templates['templates'][templateEnabled].without('template')
                template_merged = CommonHelper.deep_merge(template_from, template_to)
                templates['templates'][templateEnabled] = template_merged
              end
              templates['templates'][templateEnabled].each do | alarm,parameters |
                resourceParams = parameters.clone
                # Override template params if overrides provided
                params.each do | x,y |
                  resourceParams[x] = y
                end
                if k == :hosts
                  resourceParams['cmds'].each do |cmd|
                    hostParams = resourceParams.clone
                    hostParams['cmd'] = cmd
                    # Construct alarm object per cmd
                    alarms << {
                      resource: resource,
                      type: k[0...-1],
                      template: templateEnabled,
                      alarm: alarm,
                      parameters: hostParams,
                      environments: environments
                    }
                  end
                else
                  # Construct alarm object
                  alarms << {
                    resource: resource,
                    type: k[0...-1],
                    template: templateEnabled,
                    alarm: alarm,
                    parameters: resourceParams,
                    environments: environments
                  }
                end
              end
            end
          end
        end
      end
    end

    # Create temp alarms file for CfnDsl
    temp_file = Tempfile.new(["alarms-",'.yml'])
    temp_file_path = temp_file.path
    temp_file.write({'alarms' => alarms}.to_yaml)
    temp_file.rewind

    # Split resources for mulitple templates to avoid CloudFormation template resource limits
    split = []
    template_envs = ['production']
    alarms.each_with_index do |alarm,index|
      split[index/config['resource_limit']] ||= {}
      split[index/config['resource_limit']]['alarms'] ||= []
      split[index/config['resource_limit']]['alarms'] << alarm
      template_envs |= get_alarm_envs(alarm[:parameters])
    end

    # Create temp files for split resources for CfnDsl input
    temp_files=[]
    temp_file_paths=[]
    (alarms.count/config['resource_limit'].to_f).ceil.times do | i |
      temp_files[i] = Tempfile.new(["alarms-#{i}-",'.yml'])
      temp_file_paths[i] = temp_files[i].path
      temp_files[i].write(split[i].to_yaml)
      temp_files[i].rewind
    end

    ARGV.each { |a| task a.to_sym do ; end }

    write_cfdndsl_template(temp_file_path,temp_file_paths,alarms_file,customer,source_bucket,template_envs,output_path,upload_path, output_stream)
  end

  desc('Deploy cloudformation templates to S3')
  task :deploy do
    ARGV.each { |a| task a.to_sym do ; end }
    customer = ARGV[1]
    application = ARGV[2]

    if !customer
      puts "Usage:"
      puts "rake cfn:deploy <customer> [application]"
      exit 1
    end

    config = load_config(customer, application)
    customer_alarms_config = config['alarms']
    output_path = config['output_path']
    upload_path = config['upload_path']

    puts "-----------------------------------------------"
    s3 = Aws::S3::Client.new(region: customer_alarms_config['source_region'])
    ["#{output_path}/*.json"].each { |path|
      Dir.glob(path) do |file|
        template = File.open(file, 'rb')
        filename = file.gsub("#{output_path}/", "")
        s3.put_object({
            body: template,
            bucket: "#{customer_alarms_config['source_bucket']}",
            key: "#{upload_path}/#{filename}",
        })
        puts "INFO: Copied #{file} to s3://#{customer_alarms_config['source_bucket']}/#{upload_path}/#{filename}"
      end
    }
    puts "-----------------------------------------------"
    puts "Master stack: https://s3.#{customer_alarms_config['source_region']}.amazonaws.com/#{customer_alarms_config['source_bucket']}/#{upload_path}/master.json"
    puts "-----------------------------------------------"
  end

  desc('Query environment for monitorable resources')
  task :query do
    ARGV.each { |a| task a.to_sym do ; end }
    region = ARGV[1]
    stack = ARGV[2]
    customer = ARGV[3]
    application = ARGV[4]

    if !customer || !stack || !region
      puts "Usage:"
      puts "rake cfn:query <region> <stack> <customer> [application]"
      exit 1
    end

    config = load_config(customer, application)
    if application
      customer_alarms_config_file = "ciinaboxes/#{customer}/monitoring/#{application}/alarms.yml"
    else
      customer_alarms_config_file = "ciinaboxes/#{customer}/monitoring/alarms.yml"
    end

    # Load customer config files; continue if no file is found.
    customer_alarms_config = YAML.load(File.read(customer_alarms_config_file)) if File.file?(customer_alarms_config_file)
    customer_alarms_config ||= {}
    customer_alarms_config['resources'] ||= {}

    puts "-----------------------------------------------"
    puts "stack: #{stack}"
    puts "customer: #{customer}"
    puts "region: #{region}"
    puts "-----------------------------------------------"
    puts "Searching Stacks for Monitorable Resources"
    puts "-----------------------------------------------"

    cfClient = Aws::CloudFormation::Client.new(region: region)
    elbClient = Aws::ElasticLoadBalancingV2::Client.new(region: region)

    def query_stacks (config,cfClient,elbClient,stack,stackResources={template:{},physical_resource_id:{}},location='')
      stackResourceCount = 0
      stackResourceCountLocal = 0
      begin
        resp = cfClient.list_stack_resources({
          stack_name: stack
        })
      rescue Aws::CloudFormation::Errors::ServiceError => e
        puts "Error: #{e}"
        exit 1
      end

      resp.stack_resource_summaries.each do | resource |
        if resource['resource_type'] == 'AWS::CloudFormation::Stack'
          query = query_stacks(config,cfClient,elbClient,resource['physical_resource_id'],stackResources,"#{location}.#{resource['logical_resource_id']}")
          stackResourceCount += query[:stackResourceCount]
        end
        if config['resource_defaults'].key? resource['resource_type']
          if resource['resource_type'] == 'AWS::ElasticLoadBalancingV2::TargetGroup'
            begin
              tg = elbClient.describe_target_groups({
                target_group_arns: [ resource['physical_resource_id'] ]
              })
            rescue Aws::ElasticLoadBalancingV2::Errors::ServiceError => e
              puts "Error: #{e}"
              exit 1
            end
            stackResources[:template]["#{location[1..-1]}.#{resource['logical_resource_id']}/#{tg['target_groups'][0]['load_balancer_arns'][0]}"] = config['resource_defaults'][resource['resource_type']]
          else
            stackResources[:template]["#{location[1..-1]}.#{resource['logical_resource_id']}"] = config['resource_defaults'][resource['resource_type']]
          end
          stackResourceCount += 1
          stackResourceCountLocal += 1
          print "#{location[1..-1]}: Found #{stackResourceCount} resource#{"s" if stackResourceCount != 1}\r"
          sleep 0.2
        elsif resource['resource_type'] == 'AWS::ElasticLoadBalancingV2::LoadBalancer'
          stackResources[:physical_resource_id][resource['physical_resource_id']] = "#{location[1..-1]}.#{resource['logical_resource_id']}"
        end
      end
      stackResourceQuery = {
        stackResourceCount: stackResourceCount,
        stackResources: stackResources
      }
      sleep 0.2
      puts "#{stack if location == ''}#{location[1..-1]}: Found #{stackResourceCountLocal} resource#{"s" if stackResourceCountLocal != 1}"
      stackResourceQuery
    end

    stackResourceQuery = query_stacks(config,cfClient,elbClient,stack)
    stackResourceCount = stackResourceQuery[:stackResourceCount]
    stackResources = stackResourceQuery[:stackResources]

    configResourceCount = customer_alarms_config['resources'].keys.count
    configResources = []
    keyUpdates = []

    stackResources[:template].each do | k,v |
      if stackResources[:physical_resource_id].key? k.partition('/').last
        keyUpdates << k
      end
    end

    keyUpdates.each do | k |
      stackResources[:template]["#{k.partition('/').first}/#{stackResources[:physical_resource_id][k.partition('/').last]}"] = stackResources[:template].delete(k)
    end

    stackResources[:template].each do | k,v |
      if !customer_alarms_config['resources'].any? {|x, y| x == k}
        configResources.push("#{k}: #{v}")
      end
    end

    puts "-----------------------------------------------"
    puts "Monitorable Resources (with default templates)"
    puts "-----------------------------------------------"
    stackResources[:template].each do | k,v |
      puts "#{k}: #{v}"
    end
    puts "-----------------------------------------------"
    if configResourceCount < stackResourceCount
      puts "Missing resources (with default templates)"
      puts "-----------------------------------------------"
      configResources.each do | r |
        puts r
      end
      puts "-----------------------------------------------"
    end
    puts "Monitorable resources in #{stack} stack: #{stackResourceCount}"
    puts "Resources in #{customer} alarms config: #{configResourceCount}"
    if stackResourceCount > 0
      puts "Coverage: #{100-(configResources.count*100/stackResourceCount)}%"
    end
    puts "-----------------------------------------------"
  end

  desc("Update a customer's monitoring stack")
  task :update do
    ARGV.each { |a| task a.to_sym do ; end }
    customer = ARGV[1]
    application = ARGV[2]

    if !customer
      puts "Usage:"
      puts "rake cfn:update <customer> [application]"
      exit 1
    end

    config = load_config(customer, application)
    upload_path = config['upload_path']
    source_region = config['alarms']['source_region']
    source_bucket = config['alarms']['source_bucket']
    stack_name = config['alarms']['monitoring_stack']
    template_url = "https://s3.#{source_region}.amazonaws.com/#{source_bucket}/#{upload_path}/master.json"

    if stack_name.nil?
      puts "ERROR - name of monitoring stack not found in config."
      exit 1
    end

    cf_client = Aws::CloudFormation::Client.new(region: source_region)

    # We need to pass the parameters from the existing stack into the new stack
    begin
      stack = cf_client.describe_stacks({stack_name: stack_name})
    rescue Aws::CloudFormation::Errors::ValidationError => ex
      puts "ERROR - #{ex.class}: #{ex.message}"
      exit 1
    end

    parameters = stack[0][0].parameters
    # Use the existing template's values as the new parameters
    for param in parameters do
      param['parameter_value'] = ''
      param.use_previous_value = true
    end

    cf_client.update_stack(stack_name: stack_name, template_url: template_url, parameters: parameters, capabilities: ["CAPABILITY_IAM"])
  end

  # Get a list of the differences between the existing and the newly generated monitoring.
  # This is used to confirm any changes to a particular stack before deploying them.
  desc('Compare existing and locally generated resources')
  task :compare do
    ARGV.each { |a| task a.to_sym do ; end }
    customer = ARGV[1]

    if !customer
      puts "Usage:"
      puts "rake cfn:compare <customer>"
      exit 1
    end

    config = load_config(customer, nil)

    # Load customer config files
    upload_path = config['upload_path']
    stack_name = config['alarms']['monitoring_stack']
    source_bucket = config['alarms']['source_bucket']
    source_region = config['alarms']['source_region']

    # Check if the stack exists
    cfn = Aws::CloudFormation::Client.new(region: source_region)
    begin
      stack = cfn.describe_stacks({stack_name: stack_name})
    rescue Aws::CloudFormation::Errors::ValidationError => ex
      puts "ERROR - #{ex.class}: #{ex.message}"
      exit 1
    end

    # Use the existing template's values as the new parameters
    parameters = stack[0][0].parameters

    for param in parameters do
      param['parameter_value'] = ''
      param.use_previous_value = true
    end

    change_set_name = stack_name# + '-' + Time.now.to_i.to_s # Seconds since epoch
    template_url = "https://s3.#{source_region}.amazonaws.com/#{source_bucket}/#{upload_path}/master.json"

    changes = ''
    state = 'CREATE_IN_PROGRESS'
    set = cfn.create_change_set({stack_name: stack_name, change_set_name: change_set_name, template_url: template_url, parameters: parameters, capabilities: ["CAPABILITY_IAM"]})

    # Wait until the change set is ready; it might return the wrong response.
    loop do
      changes = cfn.describe_change_set({stack_name: stack_name, change_set_name: change_set_name})

      if changes[:status] == 'CREATE_COMPLETE'
        break
      elsif changes[:status] != 'CREATE_IN_PROGRESS'
        puts "ERROR - change set returned with status #{changes[:status]}"
        exit 1
      end
      sleep(3)
    end

    # Organise the changes for printing.
    add = []
    modify = []
    remove = []

    changes[:changes].each { |item|
      action = item[:resource_change][:action]
      resource_id = item[:resource_change][:logical_resource_id]

      case action
        when 'Add'
          add.push(resource_id)
        when 'Modify'
          modify.push(resource_id)
        when 'Remove'
          remove.push(resource_id)
        end
    }

    puts "#{add.length} resources to add: #{add}"       if add.length > 0
    puts "#{modify.length} resources to modify: #{modify}"   if modify.length > 0
    puts "#{remove.length} resources to remove: #{remove}"    if remove.length > 0

    if add.length == 0 and modify.length == 0 and remove.length == 0
      puts "Nothing to change."
    end
  end

  def write_cfdndsl_template(alarms_config,configs,customer_alarms_config_file,customer,source_bucket,template_envs,output_path,upload_path, output_stream)
    FileUtils::mkdir_p output_path
    configs.each_with_index do |config,index|
      File.open("#{output_path}/resources#{index}.json", 'w') { |file|
        file.write(JSON.pretty_generate( CfnDsl.eval_file_with_extras("templates/resources.rb",[[:yaml, config],[:raw, "template_number=#{index}"],[:raw, "source_bucket='#{source_bucket}'"],[:raw, "upload_path='#{upload_path}'"]],output_stream)))}
      File.open("#{output_path}/alarms#{index}.json", 'w') { |file|
        file.write(JSON.pretty_generate( CfnDsl.eval_file_with_extras("templates/alarms.rb",[[:yaml, config],[:raw, "template_number=#{index}"],[:raw, "template_envs=#{template_envs}"]],output_stream)))}
    end
    File.open("#{output_path}/endpoints.json", 'w') { |file|
      file.write(JSON.pretty_generate( CfnDsl.eval_file_with_extras("templates/endpoints.rb",[[:yaml, alarms_config],[:raw, "template_envs=#{template_envs}"]],output_stream)))}
    File.open("#{output_path}/hosts.json", 'w') { |file|
      file.write(JSON.pretty_generate( CfnDsl.eval_file_with_extras("templates/hosts.rb",[[:yaml, alarms_config],[:raw, "template_envs=#{template_envs}"]],output_stream)))}
    File.open("#{output_path}/master.json", 'w') { |file|
      file.write(JSON.pretty_generate( CfnDsl.eval_file_with_extras("templates/master.rb",[[:yaml, customer_alarms_config_file],[:raw, "templateCount=#{configs.count}"],[:raw, "template_envs=#{template_envs}"],[:raw, "upload_path='#{upload_path}'"]],output_stream)))}
  end

  # Load a customer's configuration files from disk
  def load_config(customer, application)
    config = {}

    # Global config files
    global_config_file = 'config/config.yml'
    global_templates_file = 'config/templates.yml'

    # Load global config files
    global_templates_config = YAML.load(File.read(global_templates_file))
    config = YAML.load(File.read(global_config_file))

    if application
      config['alarms_file'] = "ciinaboxes/#{customer}/monitoring/#{application}/alarms.yml"
      config['templates_file'] = "ciinaboxes/#{customer}/monitoring/#{application}/templates.yml"
      config['output_path'] = "output/#{customer}/#{application}"
      config['upload_path'] = "cloudformation/monitoring/#{application}"
    else
      config['alarms_file'] = "ciinaboxes/#{customer}/monitoring/alarms.yml"
      config['templates_file'] = "ciinaboxes/#{customer}/monitoring/templates.yml"
      config['output_path'] = "output/#{customer}"
      config['upload_path'] = "cloudformation/monitoring"
    end

    # Load customer config files
    if File.file?(config['alarms_file'])
      config['alarms'] = YAML.load(File.read(config['alarms_file']))
    else
      puts "Failed to load #{config['alarms_file']}"
      exit 1
    end

    # Merge customer template configs over global template configs
    if File.file?(config['templates_file'])
      customer_templates_config = YAML.load(File.read(config['templates_file']))
      config['templates'] = CommonHelper.deep_merge(global_templates_config, customer_templates_config)
    else
      config['templates'] = global_templates_config
    end

    return config
  end

end
