# This instruction will guide you to create the project on GCP from the sketch

1. Sign in to the gcp You will get the $400 free credit for 3 months.

2. Open the service account and create service account

3. Get the key.json in service account

4. Open IAM & Admin and set the role into it

- Bigquery Admin
- Storage Admin
- Storage Object Admin
- Viewer

5. Create the VM instances and copy the external IP address

# Create the Virtual Machine locally

1. Open the git bash, set the directory that you want to store the file
2. type :
<pre>
touch config
</pre>
3. Create the configuration to connect the virtual machine and create the remote machine
<pre>
Host (your host name)
HostName (external IP address)
User (name of your ssh key)
IdentityFile c:/Users/muhfa/.ssh/(your key name)
</pre>
