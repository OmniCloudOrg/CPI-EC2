# AWS CPI Extension

This is a dynamic extension (DLL) implementation for managing AWS EC2 instances and EBS volumes through the CPI (Cloud Provider Interface).

## Features

- Test AWS credentials and configuration
- List, create, delete, and manage EC2 instances
- EBS volume management (create, list, attach, detach)
- Snapshot management
- EC2 instance tags (metadata) management

## Requirements

- Rust programming environment
- AWS account with appropriate credentials configured
- AWS SDK for Rust
- Tokio runtime for async operations

## Building

```bash
cargo build --release
```

The resulting DLL will be in `target/release/cpi_aws.dll` (Windows), `.so` (Linux), or `.dylib` (macOS).

## AWS Credentials Configuration

This extension uses the standard AWS SDK credential provider chain. Credentials can be configured in several ways:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM roles for EC2 instances or ECS tasks
4. AWS SSO

## Usage

This extension implements the following actions:

### EC2 Instance Management
- `test_install`: Test if AWS credentials are properly configured
- `list_workers`: List all EC2 instances
- `create_worker`: Create a new EC2 instance
- `delete_worker`: Terminate an EC2 instance
- `get_worker`: Get information about an EC2 instance
- `has_worker`: Check if an EC2 instance exists
- `start_worker`: Start a stopped EC2 instance
- `reboot_worker`: Reboot an EC2 instance

### EBS Volume Management
- `get_volumes`: List all EBS volumes
- `has_volume`: Check if an EBS volume exists
- `create_volume`: Create a new EBS volume
- `delete_volume`: Delete an EBS volume
- `attach_volume`: Attach an EBS volume to an EC2 instance
- `detach_volume`: Detach an EBS volume from an EC2 instance
- `snapshot_volume`: Create a snapshot of an EBS volume

### Snapshot Management
- `create_snapshot`: Create a snapshot of an EBS volume
- `delete_snapshot`: Delete a snapshot
- `has_snapshot`: Check if a snapshot exists

### Metadata Management
- `set_worker_metadata`: Set metadata (tags) for an EC2 instance

## Technical Details

This extension uses the AWS SDK for Rust to interact with the EC2 API. All operations are performed asynchronously using the Tokio runtime.

### Implementation Notes

- The extension maps EC2 API responses to simplified custom structs, ignoring extraneous data
- All responses follow the same format as other CPI providers (VirtualBox, Hyper-V)
- The `id` field in responses uses the EC2 instance ID, EBS volume ID, or snapshot ID
- All async operations are executed in a single-threaded Tokio runtime
- AWS credentials are loaded from the standard credential provider chain

### Region Handling

By default, the extension uses the `us-east-1` region, but each action can accept a `region` parameter to override this.

## Error Handling

AWS API errors are caught and returned as structured error messages. The extension handles "not found" errors gracefully for exists-check operations.

## Data Mapping

The extension maps AWS API responses to simplified structures:

| AWS Object | CPI Object | ID Field |
|------------|------------|----------|
| EC2 Instance | Worker | Instance ID (i-xxxxxxxx) |
| EBS Volume | Volume | Volume ID (vol-xxxxxxxx) |
| EBS Snapshot | Snapshot | Snapshot ID (snap-xxxxxxxx) |

## Security Considerations

This extension requires AWS credentials to function. Take care to secure your AWS credentials and use the principle of least privilege when creating IAM roles or users for this extension.