use aws_sdk_ec2::operation::describe_instances::DescribeInstancesOutput;
use aws_sdk_ec2::operation::describe_volumes::DescribeVolumesOutput;
// File: cpi_aws/src/lib.rs
use lib_cpi::{
    ActionDefinition, ActionResult, CpiExtension, ParamType, param, validation
};
use serde_json::{json, Value};
use std::collections::HashMap;

// AWS SDK crates
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_ec2::Client;
use aws_sdk_ec2::config::Region;
#[allow(unused_imports)]
use aws_sdk_ec2::types::{Filter, Tag, ResourceType, InstanceType, TagSpecification};

#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
pub extern "C" fn get_extension() -> *mut dyn CpiExtension {
    Box::into_raw(Box::new(AwsExtension::new()))
}

/// AWS provider implemented as a dynamic extension
pub struct AwsExtension {
    name: String,
    provider_type: String,
    default_settings: HashMap<String, Value>,
    ec2_client: Option<Client>,
}

impl AwsExtension {
    pub fn new() -> Self {
        let mut default_settings = HashMap::new();
        default_settings.insert("region".to_string(), json!("us-east-1"));
        default_settings.insert("instance_type".to_string(), json!("t2.micro"));
        default_settings.insert("ami".to_string(), json!("ami-0c55b159cbfafe1f0")); // Default Amazon Linux 2 AMI
        default_settings.insert("availability_zone".to_string(), json!("us-east-1a"));
        default_settings.insert("volume_type".to_string(), json!("gp2"));

        Self {
            name: "ec2".to_string(),
            provider_type: "cloud".to_string(),
            default_settings,
            ec2_client: None,
        }
    }
    
    // Initialize the EC2 client
    async fn get_client(&mut self, region_str: Option<&str>) -> Result<Client, String> {
        let region = region_str.unwrap_or("us-east-1");
        
        if let Some(client) = &self.ec2_client {
            return Ok(client.clone());
        }
        
        let region_provider = RegionProviderChain::first_try(Region::new(region.to_string()))
            .or_default_provider()
            .or_else(Region::new("us-east-1"));
            
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region_provider).load().await;
        
        let client = Client::new(&shared_config);
        self.ec2_client = Some(client.clone());
        
        Ok(client)
    }
    
    // Helper function to get name tag from AWS tags
    fn get_name_from_tags(&self, tags: &[Tag]) -> String {
        for tag in tags {
            if tag.key() == Some("Name") {
                return tag.value().unwrap_or("unnamed").to_string();
            }
        }
        "unnamed".to_string()
    }
    
    // Helper function to create a name filter for queries
    // fn create_name_filter(&self, name: &str) -> Filter {
    //     Filter::builder()
    //         .name("tag:Name")
    //         .values(name)
    //         .build()
    // }
    
    // Implementation of individual actions
    
    // async fn test_install(&mut self) -> ActionResult {
    //     // Just try to get the EC2 client and list regions to verify credentials work
    //     let client = self.get_client(None).await?;
    //     
    //     let result = client.describe_regions()
    //         .send()
    //         .await
    //         .map_err(|e| format!("Failed to connect to AWS: {:?}", e))?;
    //     
    //     let regions = result.regions()
    //                         .iter()
    //                         .filter_map(|r| r.region_name().map(|s| s.to_string()))
    //                         .collect::<Vec<String>>();
    //     
    //     Ok(json!({
    //         "success": true,
    //         "version": "AWS SDK for Rust",
    //         "regions": regions
    //     }))
    // }
    
    async fn list_workers(&mut self, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        let result = client.describe_instances()
            .send()
            .await
            .map_err(|e| format!("Failed to list EC2 instances: {:?}", e))?;
        
        let workers = self.parse_ec2_instances(&result);
        
        Ok(json!({
            "workers": workers
        }))
    }
    
    // Helper to map EC2 API response to our simplified form
    fn parse_ec2_instances(&self, output: &DescribeInstancesOutput) -> Vec<Value> {
        let mut instances = Vec::new();
        
        for reservation in output.reservations() {
            for instance in reservation.instances() {
                if let Some(instance_id) = instance.instance_id() {
                    let state = instance.state()
                                .and_then(|s| s.name())
                                .map(|s| s.as_str())
                                .unwrap_or("unknown");
                    
                    let name = self.get_name_from_tags(instance.tags());
                    
                    let worker = json!({
                        "id": instance_id,
                        "name": name,
                        "state": state,
                        "instance_type": instance.instance_type().map(|t| t.as_str()).unwrap_or("unknown"),
                        "public_ip": instance.public_ip_address(),
                        "private_ip": instance.private_ip_address()
                    });
                    
                    instances.push(worker);
                }
            }
        }
        
        instances
    }
    
    async fn create_worker(&mut self, worker_name: String, instance_type: String, ami: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        // Create tags for the instance
        let tag_specifications = TagSpecification::builder()
            .resource_type(ResourceType::Instance)
            .tags(
                Tag::builder()
                    .key("Name")
                    .value(&worker_name)
                    .build()
            )
            .build();
        
        // Convert instance type string to enum
        let instance_type_enum = match instance_type.as_str() {
            "t2.micro" => InstanceType::T2Micro,
            "t2.small" => InstanceType::T2Small,
            "t2.medium" => InstanceType::T2Medium,
            "t3.micro" => InstanceType::T3Micro,
            "t3.small" => InstanceType::T3Small,
            "t3.medium" => InstanceType::T3Medium,
            "m5.large" => InstanceType::M5Large,
            "m5.xlarge" => InstanceType::M5Xlarge,
            _ => InstanceType::T2Micro, // Default to t2.micro if not matched
        };
        
        let result = client.run_instances()
            .image_id(ami)
            .instance_type(instance_type_enum)
            .min_count(1)
            .max_count(1)
            .tag_specifications(tag_specifications)
            .send()
            .await
            .map_err(|e| format!("Failed to create EC2 instance: {:?}", e))?;
        
        if let Some(instance) = result.instances().first() {
            if let Some(instance_id) = instance.instance_id() {
                return Ok(json!({
                    "success": true,
                    "id": instance_id,
                    "name": worker_name
                }));
            }
        }
        
        Err("No instance was created".to_string())
    }
    
    async fn delete_worker(&mut self, worker_id: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        client.terminate_instances()
            .instance_ids(worker_id.clone())
            .send()
            .await
            .map_err(|e| format!("Failed to terminate EC2 instance: {:?}", e))?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    async fn get_worker(&mut self, worker_id: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        let result = client.describe_instances()
            .instance_ids(worker_id.clone())
            .send()
            .await
            .map_err(|e| format!("Failed to get EC2 instance details: {:?}", e))?;
        
        if let Some(reservation) = result.reservations().first() {
            if let Some(instance) = reservation.instances().first() {
                let name = self.get_name_from_tags(instance.tags());
                let state = instance.state()
                            .and_then(|s| s.name())
                            .map(|s| s.as_str())
                            .unwrap_or("unknown");
                
                let vm_info = json!({
                    "name": name,
                    "id": instance.instance_id().unwrap_or("unknown"),
                    "state": state,
                    "instance_type": instance.instance_type().map(|t| t.as_str()).unwrap_or("unknown"),
                    "public_ip": instance.public_ip_address(),
                    "private_ip": instance.private_ip_address(),
                    "availability_zone": instance.placement().and_then(|p| p.availability_zone())
                });
                
                return Ok(json!({
                    "success": true,
                    "vm": vm_info
                }));
            }
        }
        
        Err(format!("Instance with ID {} not found", worker_id))
    }
    
    async fn has_worker(&mut self, worker_id: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        let result = client.describe_instances()
            .instance_ids(worker_id.clone())
            .send()
            .await;
        
        match result {
            Ok(output) => {
                let exists = output.reservations()
                    .iter()
                    .any(|r| !r.instances().is_empty());
                
                Ok(json!({
                    "success": true,
                    "exists": exists
                }))
            },
            Err(err) => {
                // Check if the error is a "not found" error
                if format!("{:?}", err).contains("InvalidInstanceID.NotFound") {
                    Ok(json!({
                        "success": true,
                        "exists": false
                    }))
                } else {
                    Err(format!("Failed to check if instance exists: {:?}", err))
                }
            }
        }
    }
    
    async fn start_worker(&mut self, worker_id: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        client.start_instances()
            .instance_ids(worker_id.clone())
            .send()
            .await
            .map_err(|e| format!("Failed to start EC2 instance: {:?}", e))?;
        
        Ok(json!({
            "success": true,
            "started": worker_id
        }))
    }
    
    async fn get_volumes(&mut self, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        let result = client.describe_volumes()
            .send()
            .await
            .map_err(|e| format!("Failed to list EBS volumes: {:?}", e))?;
        
        let volumes = self.parse_ec2_volumes(&result);
        
        Ok(json!({
            "success": true,
            "volumes": volumes
        }))
    }
    
    // Helper to map EC2 volumes API response to our simplified form
    fn parse_ec2_volumes(&self, output: &DescribeVolumesOutput) -> Vec<Value> {
        let mut volumes = Vec::new();
        
        for volume in output.volumes() {
            if let Some(volume_id) = volume.volume_id() {
                let attached_to = volume.attachments()
                    .get(0)
                    .and_then(|attachment| attachment.instance_id().map(|id| id.to_string()));
                
                let vol = json!({
                    "id": volume_id,
                    "path": volume_id,  // Using volume_id as path for consistency with other providers
                    "size_mb": (volume.size().unwrap_or(0) * 1024) as i64,  // Convert GB to MB
                    "state": volume.state().map(|s| s.as_str()).unwrap_or("unknown"),
                    "availability_zone": volume.availability_zone().unwrap_or("unknown"),
                    "attached_to": attached_to
                });
                
                volumes.push(vol);
            }
        }
        
        volumes
    }
    
    async fn has_volume(&mut self, volume_id: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        let result = client.describe_volumes()
            .volume_ids(volume_id.clone())
            .send()
            .await;
        
        match result {
            Ok(output) => {
                let exists = !output.volumes().is_empty();
                
                Ok(json!({
                    "success": true,
                    "exists": exists
                }))
            },
            Err(err) => {
                // Check if the error is a "not found" error
                if format!("{:?}", err).contains("InvalidVolume.NotFound") {
                    Ok(json!({
                        "success": true,
                        "exists": false
                    }))
                } else {
                    Err(format!("Failed to check if volume exists: {:?}", err))
                }
            }
        }
    }
    
    async fn create_volume(&mut self, size_gb: i64, availability_zone: String, volume_type: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        let result = client.create_volume()
            .availability_zone(availability_zone)
            .size(size_gb as i32)
            .volume_type(volume_type.as_str().into())
            .send()
            .await
            .map_err(|e| format!("Failed to create EBS volume: {:?}", e))?;
        
        if let Some(volume_id) = result.volume_id() {
            return Ok(json!({
                "success": true,
                "id": volume_id,
                "path": volume_id
            }));
        }
        
        Err("No volume ID was returned".to_string())
    }
    
    async fn delete_volume(&mut self, volume_id: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        client.delete_volume()
            .volume_id(volume_id)
            .send()
            .await
            .map_err(|e| format!("Failed to delete EBS volume: {:?}", e))?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    async fn attach_volume(&mut self, worker_id: String, volume_id: String, device_name: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        client.attach_volume()
            .instance_id(worker_id)
            .volume_id(volume_id)
            .device(device_name)
            .send()
            .await
            .map_err(|e| format!("Failed to attach EBS volume: {:?}", e))?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    async fn detach_volume(&mut self, volume_id: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        client.detach_volume()
            .volume_id(volume_id)
            .send()
            .await
            .map_err(|e| format!("Failed to detach EBS volume: {:?}", e))?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    async fn create_snapshot(&mut self, volume_id: String, snapshot_name: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        // Add tags for the snapshot
        let tags = vec![
            Tag::builder()
                .key("Name")
                .value(snapshot_name)
                .build()
        ];
        
        let desc = format!("Snapshot of {}", volume_id);
        let result = client.create_snapshot()
            .volume_id(volume_id)
            .description(desc)
            .tag_specifications(
                {
                    let mut builder = TagSpecification::builder()
                        .resource_type(ResourceType::Snapshot);
                    for tag in tags {
                        builder = builder.tags(tag);
                    }
                    builder.build()
                }
            )
            .send()
            .await
            .map_err(|e| format!("Failed to create snapshot: {:?}", e))?;
        
        if let Some(snapshot_id) = result.snapshot_id() {
            return Ok(json!({
                "success": true,
                "id": snapshot_id
            }));
        }
        
        Err("No snapshot ID was returned".to_string())
    }
    
    async fn delete_snapshot(&mut self, snapshot_id: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        client.delete_snapshot()
            .snapshot_id(snapshot_id)
            .send()
            .await
            .map_err(|e| format!("Failed to delete snapshot: {:?}", e))?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    async fn has_snapshot(&mut self, snapshot_id: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        let result = client.describe_snapshots()
            .snapshot_ids(snapshot_id.clone())
            .send()
            .await;
        
        match result {
            Ok(output) => {
                let exists = !output.snapshots().is_empty();
                
                Ok(json!({
                    "success": true,
                    "exists": exists
                }))
            },
            Err(err) => {
                // Check if the error is a "not found" error
                if format!("{:?}", err).contains("InvalidSnapshot.NotFound") {
                    Ok(json!({
                        "success": true,
                        "exists": false
                    }))
                } else {
                    Err(format!("Failed to check if snapshot exists: {:?}", err))
                }
            }
        }
    }
    
    async fn reboot_worker(&mut self, worker_id: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        client.reboot_instances()
            .instance_ids(worker_id.clone())
            .send()
            .await
            .map_err(|e| format!("Failed to reboot EC2 instance: {:?}", e))?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    async fn set_worker_metadata(&mut self, worker_id: String, key: String, value: String, region: Option<&str>) -> ActionResult {
        let client = self.get_client(region).await?;
        
        let tag = Tag::builder()
            .key(key)
            .value(value)
            .build();
        
        client.create_tags()
            .resources(worker_id.clone())
            .tags(tag)
            .send()
            .await
            .map_err(|e| format!("Failed to set instance metadata: {:?}", e))?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    async fn snapshot_volume(&mut self, source_volume_id: String, snapshot_name: String, region: Option<&str>) -> ActionResult {
        // First create a snapshot of the source volume
        let snapshot_result = self.create_snapshot(source_volume_id.clone(), snapshot_name.clone(), region).await?;
        
        // Extract the snapshot ID
        let snapshot_id = match snapshot_result.get("id") {
            Some(Value::String(id)) => id.clone(),
            _ => return Err("Failed to get snapshot ID".to_string()),
        };
        
        Ok(json!({
            "success": true,
            "id": snapshot_id,
            "source_volume_id": source_volume_id
        }))
    }
}

// This is the synchronous entry point that CPI will call
impl CpiExtension for AwsExtension {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn provider_type(&self) -> &str {
        &self.provider_type
    }
    
    fn list_actions(&self) -> Vec<String> {
        vec![
            "test_install".to_string(),
            "list_workers".to_string(),
            "create_worker".to_string(),
            "delete_worker".to_string(),
            "get_worker".to_string(),
            "has_worker".to_string(),
            "start_worker".to_string(),
            "get_volumes".to_string(),
            "has_volume".to_string(),
            "create_volume".to_string(),
            "delete_volume".to_string(),
            "attach_volume".to_string(),
            "detach_volume".to_string(),
            "create_snapshot".to_string(),
            "delete_snapshot".to_string(),
            "has_snapshot".to_string(),
            "reboot_worker".to_string(),
            "set_worker_metadata".to_string(),
            "snapshot_volume".to_string(),
        ]
    }
    
    fn get_action_definition(&self, action: &str) -> Option<ActionDefinition> {
        match action {
            "test_install" => Some(ActionDefinition {
                name: "test_install".to_string(),
                description: "Test if AWS credentials are properly configured".to_string(),
                parameters: vec![
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "list_workers" => Some(ActionDefinition {
                name: "list_workers".to_string(),
                description: "List all EC2 instances".to_string(),
                parameters: vec![
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "create_worker" => Some(ActionDefinition {
                name: "create_worker".to_string(),
                description: "Create a new EC2 instance".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the instance to create", ParamType::String, required),
                    param!("instance_type", "EC2 instance type", ParamType::String, optional, json!("t2.micro")),
                    param!("ami", "Amazon Machine Image ID", ParamType::String, optional, json!("ami-0c55b159cbfafe1f0")),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "delete_worker" => Some(ActionDefinition {
                name: "delete_worker".to_string(),
                description: "Terminate an EC2 instance".to_string(),
                parameters: vec![
                    param!("worker_id", "ID of the instance to terminate", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "get_worker" => Some(ActionDefinition {
                name: "get_worker".to_string(),
                description: "Get information about an EC2 instance".to_string(),
                parameters: vec![
                    param!("worker_id", "ID of the instance", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "has_worker" => Some(ActionDefinition {
                name: "has_worker".to_string(),
                description: "Check if an EC2 instance exists".to_string(),
                parameters: vec![
                    param!("worker_id", "ID of the instance", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "start_worker" => Some(ActionDefinition {
                name: "start_worker".to_string(),
                description: "Start an EC2 instance".to_string(),
                parameters: vec![
                    param!("worker_id", "ID of the instance to start", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "get_volumes" => Some(ActionDefinition {
                name: "get_volumes".to_string(),
                description: "List all EBS volumes".to_string(),
                parameters: vec![
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "has_volume" => Some(ActionDefinition {
                name: "has_volume".to_string(),
                description: "Check if an EBS volume exists".to_string(),
                parameters: vec![
                    param!("volume_id", "ID of the volume", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "create_volume" => Some(ActionDefinition {
                name: "create_volume".to_string(),
                description: "Create a new EBS volume".to_string(),
                parameters: vec![
                    param!("size_gb", "Size in GB", ParamType::Integer, required),
                    param!("availability_zone", "Availability zone", ParamType::String, required),
                    param!("volume_type", "Volume type (gp2, io1, etc.)", ParamType::String, optional, json!("gp2")),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "delete_volume" => Some(ActionDefinition {
                name: "delete_volume".to_string(),
                description: "Delete an EBS volume".to_string(),
                parameters: vec![
                    param!("volume_id", "ID of the volume", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "attach_volume" => Some(ActionDefinition {
                name: "attach_volume".to_string(),
                description: "Attach an EBS volume to an EC2 instance".to_string(),
                parameters: vec![
                    param!("worker_id", "ID of the instance", ParamType::String, required),
                    param!("volume_id", "ID of the volume", ParamType::String, required),
                    param!("device_name", "Device name (e.g., /dev/sdf)", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "detach_volume" => Some(ActionDefinition {
                name: "detach_volume".to_string(),
                description: "Detach an EBS volume from an EC2 instance".to_string(),
                parameters: vec![
                    param!("volume_id", "ID of the volume", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "create_snapshot" => Some(ActionDefinition {
                name: "create_snapshot".to_string(),
                description: "Create a snapshot of an EBS volume".to_string(),
                parameters: vec![
                    param!("volume_id", "ID of the volume", ParamType::String, required),
                    param!("snapshot_name", "Name of the snapshot", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "delete_snapshot" => Some(ActionDefinition {
                name: "delete_snapshot".to_string(),
                description: "Delete a snapshot".to_string(),
                parameters: vec![
                    param!("snapshot_id", "ID of the snapshot", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "has_snapshot" => Some(ActionDefinition {
                name: "has_snapshot".to_string(),
                description: "Check if a snapshot exists".to_string(),
                parameters: vec![
                    param!("snapshot_id", "ID of the snapshot", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "reboot_worker" => Some(ActionDefinition {
                name: "reboot_worker".to_string(),
                description: "Reboot an EC2 instance".to_string(),
                parameters: vec![
                    param!("worker_id", "ID of the instance", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "set_worker_metadata" => Some(ActionDefinition {
                name: "set_worker_metadata".to_string(),
                description: "Set metadata (tags) for an EC2 instance".to_string(),
                parameters: vec![
                    param!("worker_id", "ID of the instance", ParamType::String, required),
                    param!("key", "Metadata key", ParamType::String, required),
                    param!("value", "Metadata value", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            "snapshot_volume" => Some(ActionDefinition {
                name: "snapshot_volume".to_string(),
                description: "Create a snapshot of an EBS volume".to_string(),
                parameters: vec![
                    param!("source_volume_id", "ID of the source volume", ParamType::String, required),
                    param!("snapshot_name", "Name for the snapshot", ParamType::String, required),
                    param!("region", "AWS region", ParamType::String, optional, json!("us-east-1")),
                ],
            }),
            _ => None,
        }
    }
    
    fn execute_action(&self, action: &str, params: &HashMap<String, Value>) -> ActionResult {
        // Create a runtime for executing async functions
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to create runtime: {}", e))?;
        
        // Clone self to create a mutable version for the async functions
        let mut aws_ext = AwsExtension {
            name: self.name.clone(),
            provider_type: self.provider_type.clone(),
            default_settings: self.default_settings.clone(),
            ec2_client: self.ec2_client.clone(),
        };
            
        // Extract common region parameter
        let region = validation::extract_string_opt(params, "region").ok().flatten();
        let region_ref = region.as_deref();
        
        // Execute the appropriate action
        match action {
            "test_install" => runtime.block_on(async { aws_ext.test_install() }),
            
            "list_workers" => runtime.block_on(aws_ext.list_workers(region_ref)),
            
            "create_worker" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                let instance_type = validation::extract_string_opt(params, "instance_type")?.unwrap_or_else(|| "t2.micro".to_string());
                let ami = validation::extract_string_opt(params, "ami")?.unwrap_or_else(|| "ami-0c55b159cbfafe1f0".to_string());
                
                runtime.block_on(aws_ext.create_worker(worker_name, instance_type, ami, region_ref))
            },
            
            "delete_worker" => {
                let worker_id = validation::extract_string(params, "worker_id")?;
                runtime.block_on(aws_ext.delete_worker(worker_id, region_ref))
            },
            
            "get_worker" => {
                let worker_id = validation::extract_string(params, "worker_id")?;
                runtime.block_on(aws_ext.get_worker(worker_id, region_ref))
            },
            
            "has_worker" => {
                let worker_id = validation::extract_string(params, "worker_id")?;
                runtime.block_on(aws_ext.has_worker(worker_id, region_ref))
            },
            
            "start_worker" => {
                let worker_id = validation::extract_string(params, "worker_id")?;
                runtime.block_on(aws_ext.start_worker(worker_id, region_ref))
            },
            
            "get_volumes" => runtime.block_on(aws_ext.get_volumes(region_ref)),
            
            "has_volume" => {
                let volume_id = validation::extract_string(params, "volume_id")?;
                runtime.block_on(aws_ext.has_volume(volume_id, region_ref))
            },
            
            "create_volume" => {
                let size_gb = validation::extract_int(params, "size_gb")?;
                let availability_zone = validation::extract_string(params, "availability_zone")?;
                let volume_type = validation::extract_string_opt(params, "volume_type")?.unwrap_or_else(|| "gp2".to_string());
                
                runtime.block_on(aws_ext.create_volume(size_gb, availability_zone, volume_type, region_ref))
            },
            
            "delete_volume" => {
                let volume_id = validation::extract_string(params, "volume_id")?;
                runtime.block_on(aws_ext.delete_volume(volume_id, region_ref))
            },
            
            "attach_volume" => {
                let worker_id = validation::extract_string(params, "worker_id")?;
                let volume_id = validation::extract_string(params, "volume_id")?;
                let device_name = validation::extract_string(params, "device_name")?;
                
                runtime.block_on(aws_ext.attach_volume(worker_id, volume_id, device_name, region_ref))
            },
            
            "detach_volume" => {
                let volume_id = validation::extract_string(params, "volume_id")?;
                runtime.block_on(aws_ext.detach_volume(volume_id, region_ref))
            },
            
            "create_snapshot" => {
                let volume_id = validation::extract_string(params, "volume_id")?;
                let snapshot_name = validation::extract_string(params, "snapshot_name")?;
                
                runtime.block_on(aws_ext.create_snapshot(volume_id, snapshot_name, region_ref))
            },
            
            "delete_snapshot" => {
                let snapshot_id = validation::extract_string(params, "snapshot_id")?;
                runtime.block_on(aws_ext.delete_snapshot(snapshot_id, region_ref))
            },
            
            "has_snapshot" => {
                let snapshot_id = validation::extract_string(params, "snapshot_id")?;
                runtime.block_on(aws_ext.has_snapshot(snapshot_id, region_ref))
            },
            
            "reboot_worker" => {
                let worker_id = validation::extract_string(params, "worker_id")?;
                runtime.block_on(aws_ext.reboot_worker(worker_id, region_ref))
            },
            
            "set_worker_metadata" => {
                let worker_id = validation::extract_string(params, "worker_id")?;
                let key = validation::extract_string(params, "key")?;
                let value = validation::extract_string(params, "value")?;
                
                runtime.block_on(aws_ext.set_worker_metadata(worker_id, key, value, region_ref))
            },
            
            "snapshot_volume" => {
                let source_volume_id = validation::extract_string(params, "source_volume_id")?;
                let snapshot_name = validation::extract_string(params, "snapshot_name")?;
                
                runtime.block_on(aws_ext.snapshot_volume(source_volume_id, snapshot_name, region_ref))
            },
            
            _ => Err(format!("Action '{}' not found", action)),
        }
    }
}