# OS: Lab 4 - File Server

## Getting started

1. Run application and database with `docker-compose`

  ```bash
  docker-compose -f docker/docker-compose.yaml up -d 
  ```

2. Forward port to VM

  ```bash
  ./scripts/forward_to_vm.sh
  ```

> [!IMPORTANT]  
> I was using Lima VM for this project. Integration with [kernel module](https://github.com/S1riyS/os-course-lab-4) depends heavily on how you are working with it
