# LightningStore [![Build Status](https://travis-ci.org/mihasic/LightningStore.svg?branch=master)](https://travis-ci.org/mihasic/LightningStore) [![Build status](https://ci.appveyor.com/api/projects/status/082d5kff43a0keoq/branch/master?svg=true)](https://ci.appveyor.com/project/mihasic/lightningstore/branch/master) [![NuGet](https://img.shields.io/nuget/v/LightningStore.svg)](https://www.nuget.org/packages/LuceneSearch/)

Stream store and simple document store abstractions on top of LightningDB

## ChangeStream

Simple append-only stream of data. Allows to append, read forward, read backwords and get the latest checkpoint.
Supports autho-growth.

## CheckpointStore

Single file implementation to store `long?` that uses memory mapped files for faster read. Supports readonly mode.

## ObjectRepository

Abstraction on top of LMDB, that takes care of incremental growth.
Some concept of transactions is implemented.

> Transaction does not support auto-growth, so exceptions should be caught and operation can be completed without transaction.
> Some local identity map is recommended.

## CachingRepository

Implementation of the session, implementing in-memory identity map. The class abstracts transaction management for autogrowth. Transactions in LMDB bring benefits of isolations.
