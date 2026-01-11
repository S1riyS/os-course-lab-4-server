HOST_PORT=8082
VM_PORT=8888

echo "Forwarding port $HOST_PORT from host to port $VM_PORT in VM..."

ssh -N -R $VM_PORT:localhost:$HOST_PORT \
    -F ~/.lima/default/ssh.config \
    lima-default

echo "Port forwarding is stopped!"