targetScope = 'resourceGroup'

param location string = resourceGroup().location
@minLength(3)
@maxLength(20)
param prefix string
param registryName string
param keyVaultName string
param foundryResourceGroup string
param foundryAccountName string
param foundryProjectName string
param foundryLocation string
param foundryProjectDisplayName string = foundryProjectName
param foundryProjectDescription string = 'Dedicated STLC hosted worker project'
param serviceBusTags object = {}
param vnetTags object = {}
param acaSubnetDelegationName string = 'aca'
param serviceBusNamespaceName string = '${prefix}-sb'
param intakeLockDuration string = 'PT1M'
param vnetAddressPrefix string = '10.248.0.0/16'
param subnetAddressPrefix string = '10.248.0.0/23'

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: registryName
  location: location
  sku: { name: 'Basic' }
  properties: { adminUserEnabled: false }
}

resource vault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    tenantId: subscription().tenantId
    sku: { family: 'A', name: 'standard' }
    enableRbacAuthorization: true
    enablePurgeProtection: true
    softDeleteRetentionInDays: 7
    publicNetworkAccess: 'Enabled'
  }
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${prefix}-aca-mi'
  location: location
}

resource bus 'Microsoft.ServiceBus/namespaces@2024-01-01' = {
  name: serviceBusNamespaceName
  location: location
  tags: serviceBusTags
  sku: { name: 'Standard', tier: 'Standard' }
  properties: { minimumTlsVersion: '1.2' }
}

resource intake 'Microsoft.ServiceBus/namespaces/queues@2024-01-01' = {
  parent: bus
  name: 'stlc-events'
  properties: {
    requiresSession: false
    requiresDuplicateDetection: true
    duplicateDetectionHistoryTimeWindow: 'PT10M'
    defaultMessageTimeToLive: 'P14D'
    lockDuration: intakeLockDuration
    maxDeliveryCount: 10
    deadLetteringOnMessageExpiration: true
  }
}

resource hookSend 'Microsoft.ServiceBus/namespaces/queues/authorizationRules@2024-01-01' = {
  parent: intake
  name: 'stlc-hook-send'
  properties: { rights: [ 'Send' ] }
}

resource work 'Microsoft.ServiceBus/namespaces/queues@2024-01-01' = {
  parent: bus
  name: 'stlc-work-items'
  properties: {
    requiresSession: true
    requiresDuplicateDetection: true
    duplicateDetectionHistoryTimeWindow: 'PT10M'
    defaultMessageTimeToLive: 'P14D'
    lockDuration: 'PT1M'
    maxDeliveryCount: 10
    deadLetteringOnMessageExpiration: true
  }
}

resource logs 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${prefix}-logs'
  location: location
  properties: { sku: { name: 'PerGB2018' }, retentionInDays: 30 }
}

resource vnet 'Microsoft.Network/virtualNetworks@2023-11-01' = {
  name: '${prefix}-vnet'
  location: location
  tags: vnetTags
  properties: {
    addressSpace: { addressPrefixes: [ vnetAddressPrefix ] }
    subnets: [
      {
        name: 'aca-infrastructure'
        properties: {
          addressPrefix: subnetAddressPrefix
          delegations: [
            { name: acaSubnetDelegationName, properties: { serviceName: 'Microsoft.App/environments' } }
          ]
        }
      }
    ]
  }
}

resource environment 'Microsoft.App/managedEnvironments@2025-01-01' = {
  name: '${prefix}-aca-env'
  location: location
  properties: {
    appLogsConfiguration: { destination: 'azure-monitor' }
    vnetConfiguration: {
      internal: true
      infrastructureSubnetId: '${vnet.id}/subnets/aca-infrastructure'
    }
    workloadProfiles: [ { name: 'Consumption', workloadProfileType: 'Consumption' } ]
  }
}

resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'stlc-platform-logs'
  scope: environment
  properties: {
    workspaceId: logs.id
    logs: [
      { category: 'ContainerAppConsoleLogs', enabled: true }
      { category: 'ContainerAppSystemLogs', enabled: true }
    ]
    metrics: [ { category: 'AllMetrics', enabled: true } ]
    logAnalyticsDestinationType: 'Dedicated'
  }
}

module project 'project.bicep' = {
  name: '${prefix}-foundry-project'
  scope: resourceGroup(foundryResourceGroup)
  params: {
    accountName: foundryAccountName
    projectName: foundryProjectName
    location: foundryLocation
    displayName: foundryProjectDisplayName
    description: foundryProjectDescription
  }
}

output registryId string = registry.id
output registryServer string = registry.properties.loginServer
output vaultId string = vault.id
output vaultUrl string = vault.properties.vaultUri
output identityId string = identity.id
output identityClientId string = identity.properties.clientId
output identityPrincipalId string = identity.properties.principalId
output environmentId string = environment.id
output intakeQueueId string = intake.id
output workQueueId string = work.id
output busNamespace string = '${bus.name}.servicebus.windows.net'
output projectId string = project.outputs.id
output projectPrincipalId string = project.outputs.principalId
// Use the project endpoint supplied by the operator; account endpoints differ
// between Foundry account generations and must not be guessed from ARM output.
