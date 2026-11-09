param accountName string
param projectName string
param location string
param displayName string = projectName
param description string = 'Dedicated STLC hosted worker project'

resource account 'Microsoft.CognitiveServices/accounts@2025-06-01' existing = {
  name: accountName
}

resource project 'Microsoft.CognitiveServices/accounts/projects@2025-06-01' = {
  parent: account
  name: projectName
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    displayName: displayName
    description: description
  }
}

output id string = project.id
output principalId string = project.identity.principalId
