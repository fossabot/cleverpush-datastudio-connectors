function isAdminUser() {
  return true;
}

/**
 * Builds the Community Connector config.
 * @return {Config} The Community Connector config.
 * @see https://developers.google.com/apps-script/reference/data-studio/config
 */
function getConfig() {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();

  config.newInfo()
      .setId('instructions')
      .setText('Select the desired CleverPush channel.');

  var channelSelect = config.newSelectSingle()
      .setId('channel')
      .setName('CleverPush channel');
  var channels = getChannels();
  for (var i = 0; i < channels.length; i++) {
    var channel = channels[i];
    var label = channel.name + ' (' + channel.type + ')';
    channelSelect.addOption(config.newOptionBuilder().setLabel(label).setValue(channel._id))
  }

  config
      .newTextInput()
      .setId('notificationsLimit')
      .setName('Notification limit (default is 50)')
      .setHelpText('How many notifications should get catched from the server, increasing the number will increase loading times in google data studio')
      .setPlaceholder('50');

  config
      .newTextInput()
      .setId('notificationsSkip')
      .setName('Notification skip (default is 0)')
      .setHelpText('How many notifications should get skipped when requesting data from the server')
      .setPlaceholder('0');

  config.setDateRangeRequired(true);

  return config.build();
}

/**
 * Builds the Community Connector fields object.
 * @return {Fields} The Community Connector fields.
 * @see https://developers.google.com/apps-script/reference/data-studio/fields
 */
function getFields() {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  fields.newDimension()
      .setId('notification')
      .setName('Notification')
      .setType(types.TEXT);

  fields.newDimension()
      .setId('channel')
      .setName('Channel')
      .setType(types.TEXT);

  fields.newDimension()
      .setId('title')
      .setName('Title')
      .setType(types.TEXT);

  fields.newDimension()
      .setId('text')
      .setName('Text')
      .setType(types.TEXT);

  fields.newDimension()
      .setId('url')
      .setName('URL')
      .setType(types.TEXT);

  fields.newDimension()
      .setId('date')
      .setName('Date')
      .setType(types.YEAR_MONTH_DAY_HOUR);

  fields.newMetric()
      .setId('sent')
      .setName('Sent')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('delivered')
      .setName('Delivered')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('clicked')
      .setName('Clicked')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('opened')
      .setName('Opened')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('optOuts')
      .setName('Opt-Outs')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newDimension()
      .setId('tags')
      .setName('Tags')
      .setType(types.TEXT);

  fields.newDimension()
      .setId('topics')
      .setName('Topics')
      .setType(types.TEXT);

  fields.newDimension()
      .setId('segments')
      .setName('Segments')
      .setType(types.TEXT);

  return fields;
}

/**
 * Builds the Community Connector schema.
 * @param {object} request The request.
 * @return {object} The schema.
 */
function getSchema(request) {
  var fields = getFields().build();
  return {'schema': fields};
}

/**
 * Constructs an object with values as rows.
 * @param {Fields} requestedFields The requested fields.
 * @param {object[]} response The response.
 * @param {string} packageName The package name.
 * @return {object} An object containing rows with values.
 */
function responseToRows(requestedFields, response, channelId) {
  // Transform parsed data and filter for requested fields
  return response.map(function(notification) {
    var row = [];
    requestedFields.asArray().forEach(function(field) {
      switch (field.getId()) {
        case 'date':
          return row.push(notification.queuedAt ? notification.queuedAt.substr(0, 13).replace('T', '').replace(/-/g, '') : '');
        case 'sent':
          return row.push(notification.sent);
        case 'delivered':
          return row.push(notification.delivered);
        case 'clicked':
          return row.push(notification.clicked);
        case 'optOuts':
          return row.push(notification.optOuts);
        case 'opened':
          return row.push(notification.opened);
        case 'title':
          if (!notification.title && notification.messages && notification.messages.length && notification.messages[0].title) {
            return row.push(notification.messages[0].title);
          }
          return row.push(notification.title);
        case 'text':
          return row.push(notification.text);
        case 'url':
          return row.push(notification.url);
        case 'notification':
          return row.push(notification._id);
        case 'channel':
          return row.push(channelId);
        case 'tags':
          return row.push(notification.tags);
        case 'topics':
          return row.push(notification.topics);
        case 'segments':
          return row.push(notification.segments);
        default:
          return row.push('');
      }
    });
    return {values: row};
  });
}

/**
 * Gets the data for the community connector
 * @param {object} request The request.
 * @return {object} The data.
 */
function getData(request) {
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  var requestedFields = getFields().forIds(requestedFieldIds);

  const notificationsLimit = request.configParams.notificationsLimit ? request.configParams.notificationsLimit : 50;
  const notificationsSkip = request.configParams.notificationsSkip ? request.configParams.notificationsSkip : 0;

  // Fetch and parse data from API
  var url = [
    'https://api.cleverpush.com/channel/', request.configParams.channel , '/notifications?status=sent', 
    '&limit=', notificationsLimit, '&offset=', notificationsSkip
  ];
  var response = UrlFetchApp.fetch(url.join(''), {
   "method" : "get",
     "headers" : {
       "Authorization": PropertiesService.getUserProperties().getProperty('dscc.key')
     }
  });

  var parsedResponse = JSON.parse(response).notifications;
  const FILTER_NAMES = ['tags', 'topics', 'segments'];
  FILTER_NAMES.forEach((fieldName) => {
    if(requestedFieldIds.includes(fieldName)) {
      convertIdsToName(parsedResponse, request.configParams.channel, fieldName);
    }
  });
  var rows = responseToRows(requestedFields, parsedResponse, request.configParams.channel);

  return {
    schema: requestedFields.build(),
    rows: rows
  };
}

function convertIdsToName(parsedResponse, channelId, fieldName) {
  const ENTRY_LIMIT = 100;
  const MAX_PAGES = 10;
  var nameMap = {};
  for(var i=0; i<MAX_PAGES; i++) {
    var url = [
      'https://api.cleverpush.com/channel/', channelId , '/', fieldName, '?limit=', ENTRY_LIMIT, '&skip=', i * ENTRY_LIMIT
    ];
    console.log(url.join(''));
    var response = UrlFetchApp.fetch(url.join(''), {
    "method" : "get",
      "headers" : {
        "Authorization": PropertiesService.getUserProperties().getProperty('dscc.key')
      }
    });

    var entries = JSON.parse(response)[fieldName];
    if(entries.length === 0) {
      break;
    }
    
    entries.forEach((entry) => {
      nameMap[entry._id] = entry.name;
    });

    if(entries.length < ENTRY_LIMIT) {
      break;
    }
  }

  parsedResponse.forEach((notification) => {
    var convertedEntry = '';
    if(!notification[fieldName]) {
      notification[fieldName] = '';
      return;
    }

    notification[fieldName].forEach((entryId, index, arr) => {
      if(nameMap[entryId]) {
        if(index < arr.length - 1) {
          convertedEntry += nameMap[entryId] + ', '; 
        } else {
          convertedEntry += nameMap[entryId];
        }
      }
    });
    notification[fieldName] = convertedEntry;
  });
}

/**
 * Returns the Auth Type of this connector.
 * @return {object} The Auth type.
 */
function getAuthType() {
  var cc = DataStudioApp.createCommunityConnector();
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.KEY)
    .setHelpUrl('https://cleverpush.com/app/settings/api')
    .build();
}

/**
 * Resets the auth service.
 */
function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('dscc.key');
}

/**
 * Returns true if the auth service has access.
 * @return {boolean} True if the auth service has access.
 */
function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var key = userProperties.getProperty('dscc.key');
  // This assumes you have a validateKey function that can validate
  // if the key is valid.
  return validateKey(key);
}

function getChannels() {
  var response = UrlFetchApp.fetch('https://api.cleverpush.com/channels', {
   "method" : "get",
     "headers" : {
       "Authorization": PropertiesService.getUserProperties().getProperty('dscc.key')
     }
  });
  var data = JSON.parse(response.getContentText());
  return data.channels;
}

function validateKey(key) {
  if (!key) {
    return false;
  }
  var response = UrlFetchApp.fetch('https://api.cleverpush.com/channels', {
   "method" : "get",
     "headers" : {
       "Authorization": key
     }
  });
  return response.getResponseCode() === 200;
}

/**
 * Sets the credentials.
 * @param {Request} request The set credentials request.
 * @return {object} An object with an errorCode.
 */
function setCredentials(request) {
  var key = request.key;

  // Optional
  // Check if the provided key is valid through a call to your service.
  // You would have to have a `checkForValidKey` function defined for
  // this to work.
  var validKey = validateKey(key);
  if (!validKey) {
    return {
      errorCode: 'INVALID_CREDENTIALS'
    };
  }
  var userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty('dscc.key', key);
  return {
    errorCode: 'NONE'
  };
}
