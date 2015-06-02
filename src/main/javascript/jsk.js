// JavaScript to Kontraktor bridge
// matches kontraktor 3.0 json-no-ref encoded remoting
// as I am kind of a JS beginner, hints are welcome :)
window.jsk = window.jsk || (function () {

  var futureMap = {}; // future id => promise
  var currentSocket = { socket: null }; // use indirection to refer to a socket in order to ease reconnects
  var sbIdCount = 1;
  var sendSequence = 1;

  function jsk(){
  }

  var _jsk = new jsk();

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // fst-Json Helpers

  /**
   * create wrapper object to make given list a valid fst-json Java Object array or Collection for sending
   */
  jsk.prototype.buildJList = function( list ) {
    list.splice( 0, 0, list.length ); // insert number of elements at 0
    return { styp: "array", seq: list };
  };

  /**
   * create wrapper object to make given list a valid fst-json Java Object for sending
   */
  jsk.prototype.buildJObject = function( type, obj ) {
    return { typ: type, obj: obj }
  };

  /**
   * makes a fst json serialized object more js-friendly
   * @param obj
   * @param preserveTypeAsAttribute - create a _typ property on each object denoting original java type
   * @param optionalTransformer - called as a map function for each object. if returns != null => replace given object in tree with result
   * @returns {*}
   */
  jsk.prototype.transform = function(obj, preserveTypeAsAttribute, optionalTransformer) {
    if (optionalTransformer) {
      var trans = optionalTransformer.apply(null, [obj]);
      if (trans)
        return trans;
    }
    if (!obj)
      return obj;
    if (obj["styp"] && obj["seq"]) {
      var arr = this.transform(obj["seq"], preserveTypeAsAttribute, optionalTransformer);
      if (arr) {
        arr.shift();
        if (preserveTypeAsAttribute)
          arr["_typ"] = obj["styp"];
      }
      return arr;
    }
    if (obj["typ"] && obj["obj"]) {
      if ('list' === obj['typ']) {
        // remove leading element length from arraylist
        obj["obj"].shift();
      }
      var res = this.transform(obj["obj"], preserveTypeAsAttribute, optionalTransformer);
      if (preserveTypeAsAttribute)
        res["_typ"] = obj["typ"];
      return res;
    }
    for (var property in obj) {
      if (obj.hasOwnProperty(property) && obj[property] != null) {
        if (obj[property].constructor == Object) {
          obj[property] = this.transform(obj[property], preserveTypeAsAttribute, optionalTransformer);
        } else if (obj[property].constructor == Array) {
          for (var i = 0; i < obj[property].length; i++) {
            obj[property][i] = this.transform(obj[property][i], preserveTypeAsAttribute, optionalTransformer);
          }
        }
      }
    }
    return obj;
  };

  /////////////////////////////////////////////////////////////////////////////////////////////
  // actor remoting helper

  _jsk.connect = function(wsurl,errorcallback) {
    var res = new _jsk.Promise();
    var socket = new _jsk.KontraktorSocket(wsurl);
    var myHttpApp = new _jsk.KontrActor(1,"RemoteApp");
    socket.onmessage( function(message) {
      if ( errorcallback ) {
        errorcallback.apply(null,[message]);
      } else if (typeof message === MessageEvent ) {
        console.error("unexpected message:"+message.data);
      } else {
        console.error("unexpected message");
        console.log(JSON.stringify(message, null, 2));
      }
    });
    socket.onerror( function(err) {
      if ( ! res.isCompleted() )
        res.complete(null,err);
      if ( errorcallback )
        errorcallback.apply(null,[err]);
      else
        console.log(err);
    });
    socket.onclose( function() {
      if ( ! res.isCompleted() )
        res.complete(null,"closed");
      if ( errorcallback )
        errorcallback.apply(null,["closed"]);
      else
        console.log("close");
    });
    socket.onopen( function (event) {
      res.complete(myHttpApp,null);
    });
    return res;
  };

  /**
   * Minimalistic Promise class. FIXME: add timeout feature
   *
   * @param optional initialResult
   * @constructor
   */
  _jsk.Promise = function(initialResult) {
    this.res = initialResult ? [initialResult,null] : null;
    this.cb = null;
    this.nextPromise = null;
  };
  _jsk.Promise.prototype.isCompleted = function() { return this.res; };
  _jsk.Promise.prototype._notify = function() {
    var res = this.cb.apply(null,this.res);
    this.cb = null;
    if ( res instanceof _jsk.Promise ) {
      res.then(this.nextPromise);
    } else {
      this.nextPromise.complete(this.res[0],this.res[1]);
    }
  };

  _jsk.Promise.prototype.then = function(cb) {
    if ( this.cb )
      throw "double callback registration on promise";
    if ( this.res ) {
      this._notify();
    }
    else
      this.cb = cb;
    this.nextPromise = new _jsk.Promise();
    return this.nextPromise;
  };

  _jsk.Promise.prototype.complete = function(r,e) {
    if ( this.res )
      throw "double completion on promise";
    this.res = [r,e];
    if ( this.cb ) {
      this._notify();
    }
  };

  /**
   * Wrapper for callbacks from remote actor's
   *
   * @param resultsCallback
   * @constructor
   */
  _jsk.Callback = function(resultsCallback) {
    if ( ! resultsCallback ) {
      throw "must register callback before sending";
    }
    this.complete = resultsCallback;
  };


  /**
   * A wrapper for a server side Actor
   */
  _jsk.KontrActor = function( id, optionalType ) {
    this.id = id;
    this.type = optionalType ? optionalType : "untyped Actor";
    this.socketHolder = currentSocket;
  };

  /**
   * create a sequenced batch of remote calls
   */
  _jsk.KontrActor.prototype.buildCallList = function( list, seqNo ) {
    var l = list.slice();
    l.push(seqNo);
    return _jsk.buildJList(l);
  };

  /**
   *
   * @param callbackId - callback id in case method has a promise as a result
   * @param receiverKey - target actor id
   * @param args - [] of properly formatted fst-json JavaObjects
   * @returns {{typ, obj}|*}
   */
  _jsk.KontrActor.prototype.buildCall = function( callbackId, receiverKey, methodName, args ) {
    return _jsk.buildJObject( "call", { futureKey: callbackId, queue: 0, method: methodName, receiverKey: receiverKey, args: _jsk.buildJList(args) } );
  };

  _jsk.KontrActor.prototype.buildCallback = function( callbackId ) {
    return { "typ" : "cbw", "obj" : [ callbackId ] };
  };

  _jsk.KontrActor.prototype.mapCBObjects = function(argList) {
    for (var i = 0; i < argList.length; i++) {
      if ( typeof argList[i] === 'function' ) { // autogenerate Callback object with given function
        argList[i] = new _jsk.Callback(argList[i]);
      }
      if (argList[i] instanceof _jsk.Callback) {
        var callbackId = sbIdCount++;
        futureMap[callbackId] = argList[i];
        argList[i] = this.buildCallback(callbackId);
      }
    }
  };

  /**
   * call an actor method returning a promise.
   *
   * "public IPromise myMethod( arg0, arg1, .. );"
   *
   */
  _jsk.KontrActor.prototype.sendWithPromise = function( methodName, args ) {
    if ( this.socketHolder.socket === null )
      throw "not connected";
    var argList = [];
    for ( var i = 1; i < arguments.length; i++ )
      argList.push(arguments[i]);
    this.mapCBObjects(argList);
    var futID = sbIdCount++;
    var cb = new _jsk.Promise();
    futureMap[futID] = cb;
    var msg = this.buildCall( futID, this.id, methodName, argList );
    this.socketHolder.socket.send(JSON.stringify(this.buildCallList([msg],sendSequence++)));
    return cb;
  };

  /**
   * call a simple asynchronous method returning nothing
   *
   * "public void myMethod( arg0, arg1, .. );"
   */
  _jsk.KontrActor.prototype.send = function( methodName, args ) {
    if ( this.socketHolder.socket === null )
      throw "not connected";
    var argList = [];
    for ( var i = 1; i < arguments.length; i++ )
      argList.push(arguments[i]);
    this.mapCBObjects(argList);
    var msg = this.buildCall( 0, this.id, methodName, argList );
    this.socketHolder.socket.send(JSON.stringify(this.buildCallList([msg],sendSequence++)));
    return this;
  };

  /**
   * Websocket wrapper class. Only difference methods are used instead of properties for onmessage, onerror, ...
   *
   * onmessage parses messages received. If a promise response is received, the promise is invoked. If onmessage
   * receives unrecognized messages, these are passed through
   *
   * @param url
   * @param protocols
   * @constructor
   */
  _jsk.KontraktorSocket = function( url, protocols ) {
    var self = this;

    currentSocket.socket = self;

    var incomingMessages = [];
    var inParse = false;

    self.automaticTransformResults = true;

    if ( protocols )
      self.socket = new WebSocket(url,protocols);
    else
      self.socket = new WebSocket(url);

    self.close = function( code, reaseon ) {
      self.socket.close(code,reaseon);
    };

    self.send = function( data ) {
      self.socket.send(data);
    };

    self.onclose = function( eventListener ) {
      self.socket.onclose = eventListener;
    };

    self.onmessage = function (eventListener) {
      self.socket.onmessage = function (message) {
        if (typeof message.data == 'string') {
          eventListener.apply(self, [message]);
        } else {
          incomingMessages.push(message.data);
          // in order to parse binary messages, an async file reader must be used.
          // therefore its necessary to ensure proper ordering of parsed messages.
          // approch taken is to parse the next message only if the previous one has been
          // parsed and processed.
          var parse = function () {
            var fr = new FileReader();
            fr.onabort = function (error) {
              if (self.socket.onerror)
                self.socket.onerror.apply(self, [error]);
              else {
                console.log("unhandled transmission error: " + error);
              }
              if (incomingMessages.length > 0)
                parse.apply();
              else
                inParse = false;
            };
            fr.onerror = fr.onabort;
            fr.onloadend = function (event) {
              try {
                var blob = event.target.result;
                var response = JSON.parse(blob);
                var respLen = response.seq[0] - 1; // last one is sequence. FIXME: should do sequence check here
                for (var i = 0; i < respLen; i++) {
                  var resp = response.seq[i + 1];
                  if (!resp.obj.method && resp.obj.receiverKey) { // => callback
                    var cb = futureMap[resp.obj.receiverKey];
                    if (!cb) {
                      console.error("unhandled callback " + JSON.stringify(resp, null, 2));
                    } else {
                      if (cb instanceof _jsk.Promise || (cb instanceof _jsk.Callback && resp.obj.args.seq[2] !== 'CNT'))
                        delete futureMap[resp.obj.receiverKey];
                      if (self.automaticTransformResults) {
                        var transFun = function (obj) {
                          if (obj != null && obj instanceof Array && obj.length == 2 && typeof obj[1] === 'string' && obj[1].indexOf("_ActorProxy") > 0) {
                            return new _jsk.KontrActor(obj[0], obj[1]); // automatically create remote actor wrapper
                          }
                          return null;
                        };
                        cb.complete(_jsk.transform(resp.obj.args.seq[1], true, transFun), _jsk.transform(resp.obj.args.seq[2], transFun)); // promise.complete(result, error)
                      } else
                        cb.complete(resp.obj.args.seq[1], resp.obj.args.seq[2]); // promise.complete(result, error)
                    }
                  } else {
                    eventListener.apply(self, [resp]);
                  }
                }
              } catch (err) {
                console.error("unhandled decoding error:" + err);
                if (self.socket.onerror)
                  self.socket.onerror.apply(self, [err]);
              }
              if (incomingMessages.length > 0)
                parse.apply();
              else
                inParse = false;
            };
            fr.readAsBinaryString(incomingMessages.shift());
          }; // end parse function
          if (!inParse) {
            inParse = true;
            parse.apply();
          }
        }
      }
    };

    self.onerror = function (eventListener) {
      self.socket.onerror = eventListener;
    };

    self.onopen = function (eventListener) {
      self.socket.onopen = function (ev) {
        setTimeout(function () {
          eventListener.apply(self.socket, [ev])
        }, 500); // FIXME: wait for correct state instead of dumb delay
      };
    };

  }; // KontraktorSocket

  return _jsk;
}());