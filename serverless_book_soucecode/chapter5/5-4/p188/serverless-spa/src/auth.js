import appConfig from './config'
import * as AWS from "aws-sdk";
import {
    CognitoUserPool,
    CognitoUserAttribute,
    AuthenticationDetails,
    CognitoUser
} from "amazon-cognito-identity-js";
export default {
    signup: function (username, email, password) {
        var _this = this
        var poolData = {
            UserPoolId: appConfig.UserPoolId,
            ClientId: appConfig.UserPoolClientId
        };
        var userPool = new CognitoUserPool(poolData);
        var attributeList = [];
        var dataEmail = {
            Name: 'email',
            Value: email
        }
        var attributeEmail = new CognitoUserAttribute(dataEmail);
        attributeList.push(attributeEmail);
        return new Promise((resolve, reject) => {
            userPool.signUp(username, password, attributeList, null,
                function (err, result) {
                    if (err) {
                        console.log(err);
                        reject(err)
                    } else {
                        console.log('username is ' + result.user.
                            getUsername());
                        resolve(result)
                    }
                })
        })
    },
    confirm: function (username, confirmation_number) {
        var _this = this
        var poolData = {
            UserPoolId: appConfig.UserPoolId,
            ClientId: appConfig.UserPoolClientId
        };
        var userPool = new CognitoUserPool(poolData);
        var userData = {
            Username: username,
            Pool: userPool
        };
        var cognitoUser = new CognitoUser(userData);
        return new Promise((resolve, reject) => {
            cognitoUser.confirmRegistration(confirmation_number,
                true, function (err, result) {
                    if (err) {
                        console.log(err)
                        reject(err)
                    } else {
                        console.log('call result: ' + result);
                        _this.onChange(true)
                        resolve(result)
                    }
                });
        })
    },
    onChange: function () { }
}