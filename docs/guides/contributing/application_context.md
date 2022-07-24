# Application Context

## Assuming roles in AWS
This point is only applicable if the infra has been set up with our `iam-config` infra block.

To gain the admin privileges necessary for AWS, one needs to assume admin role. This will be enabled only for users
defined in the infra config after logging in the AWS console for the first time as an IAM user and enabling the MFA.

Assuming roles for the first time:
1) Log in
2) Enable MFA
3) Log out
4) Log in and introduce MFA code
5) Go to the user menu in the top right corner of the AWS console
6) Click on `Switch role`
7) Input the AWS account number
8) Input the role name (`resource-admin`/`resource-user`)
9) Click on `Switch Role`

### Considerations
- The admin role expires after 1 hour
- The user role expires after 2 hours
- Once a role has expired, all the access is revoked. To get access again you will need to log out and log in
- The user role has less access than the admin one, to check on specifics go to the AWS IAM role definitions or
use the [AWS Policy Simulator](https://policysim.aws.amazon.com/home/index.jsp?#)

## Adding/Deleting scopes for client apps

### Adding or deleting a new scope

In Terraform, we define scopes (sensitivity permission levels for _client apps_).

In `modules/auth/variables.tf` we define a list of scopes. When running the Terraform these are created in Cognito and
can be assigned to _client apps_.


In order to grant a client app one or more of these scopes, you need to:

- Add/delete scopes from the list in Terraform
- Update the code in `api/application/services/authorisation_service.py` that matches client app permissions to handle the new scope(s)

> ℹ️ At the moment, we don't restrict to a domain/dataset level since the maximum number of scopes per client app
> in Cognito is 100.

> ⚠️ Note: These scopes are not directly related to user permissions. Although the action portion of the scope may be named the same (`READ`, `WRITE`, etc.)
> a user is granted permission by assigning the user to a user group whose structure is slightly different and restricts access at a dataset level e.g.: `WRITE/{domain}/{dataset}`

### Adding or removing a scope from a client app

Once the new scope has been created, you can add it to a client app.

To do so:
1. In Cognito, go to `Rapid User Pool > App Integration > App clients and analytics`
2. Choose the relevant client app
3. Go to `Hosted UI > Edit`
4. Scroll to the bottom and update the selected assigned scopes

## Distinction between scopes and user groups

Scopes are assigned on a client app level, and we grant access on an endpoint level while the user groups are assigned
to Cognito users and grant access on domain/dataset level. A user can have only 25 groups - less than the client app but
many user groups can be created and assigned to different users.

Important: authentication token can contain either custom scopes or user groups. If both are defined in the client app,
scopes take precedence.

## Adding more statements into the firewall rules

We have enabled WAF rule to protect the application. It contains two statements: one allows access to the load balancer
only from our domain name and the second protects from SQL injection. You can add more statements into the WAF rule at
no extra cost. WAF rules are defined in `modules/app-cluster/load balancer.tf`
