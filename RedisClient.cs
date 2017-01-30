﻿using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System;
using UnityEngine;
using TeamDev.Redis;

/*
 * Redis Client Wrapper for Unity.
 * required library [ TeamDev.Redis.dll ].
 * https://www.nuget.org/packages/TeamDev.Redis.Client/
 */ 

public class RedisClient : MonoBehaviour
{
	public string host = "127.0.0.1";
	public int port = 6379;
	public bool connectOnStart = true;

	public enum CLIENT_TYPE
	{
		READER = 0,
		WRITER,
		PUBLISHER,
		SUBSCRIBER
	}
	public CLIENT_TYPE clientType = CLIENT_TYPE.READER;
	private CLIENT_TYPE cliantAs;
	
	private RedisDataAccessProvider redis = null;

	private bool isConnect = false;
	public bool IsConnect { get{ return isConnect;} }

#region Pub/Sub

	// スレッドでのデータ受信
	private Thread pubsubSendThread = null;

	// スレッドでのデータ受信
	private volatile bool isDataReceived = false;
	private string receivedChannel = "";
	private string receivedMessage = "";

	// PubSubデータ受信イベント
	public delegate void OnReceivedPubSubMessageDelegate(string channel, string message);
	public event OnReceivedPubSubMessageDelegate OnReceivedPubSubMessage;
	private void InvokeOnReceived(string channel, string message)
	{
		if(OnReceivedPubSubMessage != null)
			OnReceivedPubSubMessage(channel, message);
	}

#endregion


	void Awake()
	{

	}

	void Start()
	{
		cliantAs = clientType;

		if(connectOnStart)
			Connect();
	}
	
	void Update()
	{
		// 受信イベントをメインスレッドで実行
		if(isDataReceived)
		{
			InvokeOnReceived(receivedChannel, receivedMessage);
			isDataReceived = false;
		}
	}

	void OnApplicationQuit()
	{
		Close();
	}


	// 接続
	public void Connect()
	{
		try
		{
			redis = new RedisDataAccessProvider();
			redis.Configuration.Host = host;
			redis.Configuration.Port = port;
			redis.Connect();
			redis.MessageReceived += messageReceived;

			if(redis == null)
				return;

			Debug.Log(string.Format("RedisClient :: connected to [ {0} : {1} ]", host, port.ToString()));

			isConnect = true;
		}
		catch(Exception err)
		{
			Debug.LogError("RedisClient :: " + err.Message);
		}
	}

	// 切断
	public void Close()
	{
		if(pubsubSendThread != null)
		{
			pubsubSendThread.Join();
			pubsubSendThread = null;
		}

		if(redis != null)
		{
			redis.MessageReceived -= messageReceived;
			redis.Close();
			redis.Dispose();
			redis = null;
		}

		isConnect = false;
	}

#region Set/Get string

	public void Set(string key, string value)
	{
		if(cliantAs == CLIENT_TYPE.WRITER)
		{
			if(isConnect && redis != null)
			{
				redis.SendCommand(RedisCommand.SET, key, value);
				redis.WaitComplete();
			}
		}
		else
			Debug.LogWarning("RedisClient :: Set can only client type [ CLIENT_TYPE.WRITER ]");
	}

	public string Get(string key)
	{
		string ret = "";

		if(cliantAs == CLIENT_TYPE.READER)
		{
			if(isConnect && redis != null)
			{
				redis.SendCommand(RedisCommand.GET, key);
				redis.WaitComplete();

				ret = redis.ReadString();
			}
		}
		else
			Debug.LogWarning("RedisClient :: Get can only client type [ CLIENT_TYPE.READER ]");

		return ret;
	}

#endregion

#region Pub/Sub

	public void Subscribe(params string[] channels)
	{
		if(cliantAs == CLIENT_TYPE.SUBSCRIBER)
		{
			if(isConnect && redis != null)
				redis.Messaging.Subscribe(channels);
		}
		else
			Debug.LogWarning("RedisClient :: Subscribe can only client type [ CLIENT_TYPE.SUBSCRIBER ]");
	}

	public void Publish(string channel, string message)
	{
		if(cliantAs == CLIENT_TYPE.PUBLISHER)
		{
			if(isConnect && redis != null)
			{	
				try
				{
					// 送信スレッドを開始
					if(pubsubSendThread != null)
					{
						pubsubSendThread.Join();
						pubsubSendThread = null;
					}
					pubsubSendThread = new Thread(new ThreadStart(() => {
						// 送信
						redis.Messaging.Publish(channel, message);
					}));
					pubsubSendThread.IsBackground = true;
					pubsubSendThread.Start();
				}
				catch(Exception err)
				{
					Debug.LogError(err.Message);
				}
			}
		}
		else
			Debug.LogWarning("RedisClient :: Publish can only client type [ CLIENT_TYPE.PUBLISHER ]");
	}

	private void messageReceived(string channel, string message)
	{
		isDataReceived = true;

		receivedChannel = channel;
		receivedMessage = message;
		
		//Debug.Log(string.Format("RedisClient :: channel is [ {0} ] / message is [ {1} ]", channel, message));
	}

#endregion
}
