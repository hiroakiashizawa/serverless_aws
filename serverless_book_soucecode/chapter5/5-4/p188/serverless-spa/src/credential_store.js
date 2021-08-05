export default {
    debug: true,
    state: {
      email: '',
      username: ''
    },
    setEmailAction: function(newValue) {
      if (this.debug) console.log('setMessageAction triggered with', newValue)
      this.state.email = newValue
    },
    clearEmailAction: function() {
      if (this.debug) console.log('clearMessageAction triggered')
      this.state.email = ''
    },
    setUsernameAction: function(newValue) {
      if (this.debug) console.log('setMessageAction triggered with', newValue)
      this.state.username = newValue
    },
    clearUsernameAction: function() {
      if (this.debug) console.log('clearMessageAction triggered')
      this.state.username = ''
    }
  }