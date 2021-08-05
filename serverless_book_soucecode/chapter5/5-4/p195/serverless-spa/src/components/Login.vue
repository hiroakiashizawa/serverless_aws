<template>
  <div id="login">
    <h2>Login</h2>
    <form v-on:submit.prevent="login" class="pure-form pure-form-stacked">
      <label><input v-model="email" placeholder="Email"></label>
      <label><input v-model="password" placeholder="Password" type="password"></label><br>
      <button type="submit" class="pure-button pure-button-primary">ログイン</button>
      <p>新たに<router-link to="signup">サインアップ</router-link>する</p>
      <p v-if="error" class="error">ログインに失敗しました</p>
    </form>
  </div>
</template>

<script>
import axios from 'axios'
import auth from '../auth'

export default {
  
  data: function() {
    return {
        email: "",
        password: "",
        error: false,
    }
  },

  methods: {
    login: function() {
      let _this = this
      auth.authenticate(this.email, this.password).then(function(res){
        console.log(res)
        _this.$router.replace('/home')
      }).catch(function(err){
        console.log(err)
        _this.error = true
      })
    },
  }
};
</script>

<style>
#login {
	padding: .5em 1em;
}
</style>