#!/usr/bin/ruby2.0

require 'json'
require 'net/http'
require 'uri'
require 'time'
require 'mail'
require 'optparse'
require 'aws-sdk'

options = { :role_arns => [] }
OptionParser.new do |opts|
  opts.banner = "Usage: #{__FILE__} [options]"
  opts.on("--unreported-alert u", "-u", "Alert method for unreported nodes. Supply a recipient email address. If not specified, check won't run.") do |u|
    options[:unreported] = u
  end
  opts.on("--late-alert l", "-l", "Alert method for late nodes. Supply a recipient email address. If not specified, check won't run.") do |l|
    options[:late] = l
  end
  opts.on("--grace-period n", "-g", "Grace period (in minutes) before nodes are considered late.") do |g|
    options[:grace] = g
  end
  opts.on("--corrective-alert c", "-c", "Alert method for nodes which are constantly running corrective changes. Supply a recipient email address. If not specified, check won't run.") do |c|
    options[:corrective] = c
  end
  # i.e puppet-node-checks.rb --role_arns arn:aws:iam::123456789012:role/CrossAccountReadOnly,arn:aws:iam::098765432109:role/CrossAccountReadOnly,arn:aws:iam::765432109876:role/CrossAccountReadOnly,arn:aws:iam::432109876543:role/CrossAccountFull
  opts.on("--role_arns x,y,z", Array, "IAM roles to assume when querying EC2 instances") do |r|
    options[:role_arns] = r
  end
end.parse!

puppetdb_host = 'puppet.example.com'
email = "ops@example.com"
pagerduty = 'puppet-unreported-nodes@example.pagerduty.com'

nodes_query_uri = "http://#{puppetdb_host}:8080/pdb/query/v4/nodes"
reports_query_uri = "http://#{puppetdb_host}:8080/pdb/query/v4/reports"
grace_period  = options[:grace].to_i

#################################################

module RbPuppetDB

  class PuppetNode
    attr_reader :hostname, :certname, :deactivated, :facts_environment,
                :report_environment, :catalog_environment, :facts_timestamp,
                :report_timestamp, :catalog_timestamp, :latest_report_noop,
                :latest_report_status, :expired

    def initialize(h)
      @hostname             = h["certname"].split(".")[0]
      @certname             = h["certname"]
      @deactivated          = h["deactivated"]
      @facts_environment    = h["facts_environment"]
      @report_environment   = h["report_environment"]
      @catalog_environment  = h["catalog_environment"]
      @facts_timestamp      = h["facts_timestamp"]
      @report_timestamp     = h["report_timestamp"]
      @catalog_timestamp    = h["catalog_timestamp"]
      @latest_report_noop   = h["latest_report_noop"]
      @latest_report_status = h["latest_report_status"]
      @expired              = h["expired"]
    end
  end

  def self.get_nodes_by_report_status(q_uri, status)
    # Returns a node object for all nodes where last report status was <status>.
    puppetdb_query = "[\"=\", \"latest_report_status\", \"#{status}\"]"
    uri = URI.parse(q_uri)
    params = { :query => "#{puppetdb_query}" }
    uri.query = URI.encode_www_form(params)
    response = Net::HTTP.get_response(uri)
    p_response = JSON.parse(response.body)
    # Create node objects from response.
    nodes = p_response.map { |h| PuppetNode.new(h) }
  end

  def self.get_unreported_nodes(q_uri, stopped)
    # Returns a node object for all nodes where there is no report in the database.
    response = Net::HTTP.get_response(URI(q_uri))
    p_response = JSON.parse(response.body)
    # Create node objects from response.
    nodes = p_response.map { |h| PuppetNode.new(h) }
    # Determine unreported nodes.
    nodes.keep_if { |n| n.report_timestamp.nil? }
    # Remove nodes that are stopped on purpose
    nodes.delete_if { |n| stopped.include? n.hostname }
  end

  def self.get_late_nodes(q_uri, grace, stopped)
    # Returns an array of all nodes where the last report was more than <grace> minutes ago.
    now = Time.now.to_i
    response = Net::HTTP.get_response(URI(q_uri))
    p_response = JSON.parse(response.body)
    # Create node objects from response.
    nodes = p_response.map { |h| PuppetNode.new(h) }
    # Filter out all the unreported nodes.
    nodes.delete_if { |n| n.report_timestamp.nil? }
    # Test for late nodes.
    nodes.keep_if { |n| ((now - Time.parse(n.report_timestamp).to_i) / 60) > grace }
    # Remove nodes that are stopped on purpose
    nodes.delete_if { |n| stopped.include? n.hostname }
  end

  def self.check_corrective(n_q_uri, r_q_uri, grace)
    # Returns a node object for each node where there are no reports at all in the db with status "unchanged".
    corrective_nodes = Array.new
    response = Net::HTTP.get_response(URI(n_q_uri))
    p_response = JSON.parse(response.body)
    # Create node objects from response.
    all_nodes = p_response.map { |h| PuppetNode.new(h) }

    all_nodes.each do |n|
      # Query PuppetDB for all reports for this node, aggregated by status.
      puppetdb_query = "[\"extract\",[[\"function\",\"count\"],\"status\"], [\"=\",\"certname\",\"#{n.certname}\"],[\"group_by\", \"status\"]]"
      r_uri = URI.parse(r_q_uri)
      params = { :query => "#{puppetdb_query}" }
      r_uri.query = URI.encode_www_form(params)
      n_response = Net::HTTP.get_response(r_uri)
      pn_response = JSON.parse(n_response.body)

      # Check for presence of "unchanged".
      unchanged_found = false
      pn_response.each do |pn|
        if pn['status'] == "unchanged"
          unchanged_found = true
        end
      end

      # If not found, add node to list of problem nodes.
      if unchanged_found == false
        corrective_nodes.push(n)
      end
    end

    # Filter out all the unreported and late nodes.
    corrective_nodes.delete_if { |n| n.report_timestamp.nil? or ((Time.now.to_i - Time.parse(n.report_timestamp).to_i) / 60) > grace }

    return(corrective_nodes)
  end
end

def send_email(recipient, subject, nodes)
  # Sends email alert containing list of noteworthy nodes.
  nodes.map! { |n| "#{n.hostname}\n" }
  nodes.unshift("\n")
  nodes.push("\nThis is not urgent; fix during work hours")

  mail = Mail.new do
    from "puppet_police@example.com"
    to recipient
    subject "These nodes are #{subject}"
    text_part do
      body nodes
    end
  end

  mail.deliver!
end

# Returns a list of names for EC2 instances that are purposely stopped
def aws_ec2_stopped(role_arns)

  arr = Array.new

  sts = Aws::STS::Client.new()

  role_arns.each do |a|
    credentials = Aws::AssumeRoleCredentials.new(
      client: sts,
      role_arn: a,
      role_session_name: a.gsub(/^.*\//,"") + "_session"
    )

    ec2 = Aws::EC2::Client.new(credentials: credentials)

    d = ec2.describe_instances(
      filters: [
        {
          name: "tag:InUse",
          values: ["false"],
        },
        {
          name: "tag:AlwaysOn",
          values: ["false"],
        },
        {
          name: "instance-state-name",
          values: ["stopped"]
        },
      ],
    )

    d.reservations.each do |r|
      r.instances.each do |i|
        h = aws_ec2_tags_to_h(i.tags)
        arr.push(h["Name"])
      end
    end

  end

  return arr
end

# Convert an array of Aws::EC2::Types::Tag to a hash
def aws_ec2_tags_to_h(tags)
  return_hash = {}
  tags.each do |t|
    return_hash[t.key] = t.value
  end
  return return_hash
end

#################################################

# Get the names of EC2 instances that are stopped on purpose
stopped = aws_ec2_stopped(options[:role_arns])

if options[:unreported]
  unreported_nodes = RbPuppetDB.get_unreported_nodes(nodes_query_uri, stopped)
  unless unreported_nodes.empty?
    send_email(options[:unreported], "unreported", unreported_nodes)
  end
end

if options[:late]
  late_nodes = RbPuppetDB.get_late_nodes(nodes_query_uri, grace_period, stopped)
  unless late_nodes.empty?
    send_email(options[:late], "late to report", late_nodes)
  end
end

if options[:corrective]
  corrective_nodes = RbPuppetDB.check_corrective(nodes_query_uri, reports_query_uri, grace_period)
  unless corrective_nodes.empty?
    send_email(options[:corrective], "repeatedly applying corrective changes", corrective_nodes)
  end
end
