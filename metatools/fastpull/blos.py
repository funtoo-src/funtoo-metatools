#!/usr/bin/python3

import os
from enum import Enum

import pymongo
from pymongo import MongoClient

from metatools.hashutils import calc_hashes


class BLOSError(Exception):
	pass


class BLOSObjectAlreadyExists(BLOSError):
	pass


class BLOSHashError(BLOSError):

	def __init__(self, invalid_hashes):
		self.invalid_hashes = invalid_hashes


class BLOSCorruptionError(BLOSHashError):
	"""
	This will be raised in circumstances where any of the calculated on-disk hashes do not match
	what is recorded on the MongoDB record.

	To maintain integrity of the BLOS, this file will automatically be removed. But this exception
	will still be raised to notify the caller of the issue.

	TODO: "quarantine" the corrupt file, instead?
	"""
	pass


class BLOSNotFoundError(BLOSError):
	pass


class BLOSInvalidRequest(BLOSError):
	pass


class BLOSIncompleteRecord(BLOSError):
	pass


class BackFillStrategy(Enum):
	NONE = 0
	DESIRED = 1
	ALL = 2


class BLOSResponse:

	def __init__(self, path: str = None, checked_hashes: set = None):
		self.path = path
		self.checked_hashes = checked_hashes


class BaseLayerObjectStore:
	fastpull_path = None
	spider = None
	req_client_hashes = {"sha512"}
	req_blos_hashes = {"sha512"}
	desired_hashes = {"sha512"}
	disk_verify = {"sha512"}

	backfill = BackFillStrategy.DESIRED

	def __init__(self, fastpull_path=None, spider=None):
		mc = MongoClient()
		self.collection = self.c = mc.db.fastpull
		self.collection.create_index([("hashes.sha512", pymongo.ASCENDING)])
		self.fastpull_path = fastpull_path
		self.spider = spider

		# The hashes we require in MongoDB records include those we demand in all client 'get
		# object' requests:
		self.req_blos_hashes = self.req_client_hashes | self.req_blos_hashes | {"size"}

		self.disk_verify = self.disk_verify | {"size"}

		# The hashes we desire in db include those we require, plus those we need to verify on disk.
		self.desired_hashes = self.req_blos_hashes | self.desired_hashes | self.disk_verify

		"""
		``self.disk_verify``

		This is a set of hashes we will check in the actual file on disk before returning the object, 
		to ensure the integrity of the on-disk file. Filesize (aka the 'size hash') is always turned on 
		since it's an inexpensive operation.

		These on-disk values will be checked against our MongoDB BLOS record. 

		By default we will verify disk contents using SHA512 on every read. Setting this to an empty set
		causes only filesize to be checked and will improve retrieval performance at the cost of integrity.
		Increasing the number of hashes will theoretically improve integrity checking at the cost of 
		performance. Any hash we want to check on disk will automatically also get stored in the MongoDB
		records for all new BLOS objects, since this is a requirement for future validation.

		```self.req_client_hashes```

		These are the specific hashes that must be specified for object retrieval to succeed. "size"
		can be used to specify filesize of the object, even though it is not a hash. By default we 
		only need a sha512 (not filesize) in the request. Note that these are just the *required*
		hashes for the client request to succeed, so:

		1. *All specified hashes* in ``self.req_client_hashes`` must be provided in each 'get object'
		   request, and we will use these hashes to verif
		
		2. *Any additional hashes provided* will also be used for verification, if we happen to have
		   them in our MongoDB BLOS record -- AND WILL BE IGNORED OTHERWISE. "size" is one that is
		   supported by default if you want to add that, since we always record that in MongoDB.
		   If you do, then any object you retrieve will need to include a sha512 and size for the
		   request to even be processed by the BLOS. 

		Consider the capitalized phrase "AND WILL BE IGNORED OTHERWISE" above. This may seem 
		'insecure', but the BLOS is intended to be configured to enforce a desired security 
		policy. That security policy is controlled by these settings, not what hashes the
		client happens to send to it.

		Anything 'extra' you provide beyond this configured security level is 'bonus' and will not 
		be ignored if the extra supplied hashes happen to exist in the MongoDB record -- we won't 
		knowingly return an object that appears to have a hash mismatch -- but if the BLOS is not
		calculating these hashes due to configuration, then it will not have the internal data to
		verify these hashes, and will IGNORE THEM.

		For day-to-day use of the BLOS, this means you can give it "all the hashes you've got" and 
		let it take care of enforcing its security policies. This is actually a good thing, as it
		allows you to have your code just use the BLOS and let the BLOS be a 'control point' for 
		enforcing security policies.

		If you don't like default BLOS settings, then that is a good indication that you should 
		change its default security policies to reflect what you want. That's why these settings 
		exist and are verbosely documented :)

		```self.req_blos_hashes```

		This is similar to ``self.req_client_hashes`` but refers to the MongoDB BLOS records -- if these
		fields don't exist, then the MongoDB BLOS record is considered incomplete. "size" is assumed
		and doesn't need to be specified. Anything in ``self.req_client_hashes`` is added to this
		set, because we need hashes in our MongoDB BLOS records to properly satisfy the integrity
		checks we perform between client and BLOS.

		```self.desired_hashes```

		Ideally, what hashes do we want to have in our MongoDB BLOS records? That's what is specified
		here. Filesize is assumed and doesn't need to be included via 'size'. By default we will want
		sha512 too, plus any hashes listed in ``self.disk_verify`` since we will need those for disk
		verification.

		Consider this what you want the BLOS to store and be capable of using for its own disk
		integrity checks, even if those disk integrity checks may not yet be turned on.

		How we behave when a BLOS record doesn't contain the required hashes is controlled by the 
		following setting.

		``self.backfill``

		Do we expect our MongoDB BLOS records to be complete and correct, or do we allow the BLOS
		to automatically add missing hashes to its records? This is controlled by the backfill
		strategy. This should normally be set to the default setting of::

		  BackFillStrategy.DESIRED

		This default setting of ``BackFillStrategy.DESIRED`` means that if any of our desired hashes
		in ``self.desired_hashes`` (and augmented by ``self.disk_verify``) are missing from our
		MongoDB BLOS record, go ahead and add them to our BLOS record to further expand our collections
		of hashes used for integrity checks. This will be done in real-time as objects are retrieved.

		``BackFillStrategy.ALL`` should not generally be used but can be used when you have wiped
		your MongoDB BLOS, and want to offer a bunch of existing files. The MongoDB BLOS fields will
		be reconstructed in their entirety as objects are requested by SHA512. It's tempting to
		say 'this is not secure', and may not be, unless you totally trust your files on disk,
		which you might. Under regular circumstances you do not need to enable this option -- it's
		only to hold on to old disk data when you've lost your MongoDB data.

		``BackFillStrategy.NONE`` is a super-strict option and means that all MongoDB BLOS records
		should not be auto-upgraded at all. If ``self.desired_hashes`` has been 'enhanced' to
		include more hashes not found in MongoDB BLOS records, then administrator action will be
		required to add missing hash data before the BLOS is usable again. All object retrieval
		requests will fail until this is done. 

		For example, if::

		   desired_hashes = ( "size", "sha512", "blake2b" )
		   req_client_hashes = ( "sha512" )

		Then will we automatically add blake2b hashes to MongoDB BLOS records as objects are retrieved?
		This setting works in conjunction with ``self.desired_hashes``.

		"""

	def get_disk_path(self, sha512):
		return os.path.join(self.fastpull_path, sha512[:2], sha512[2:4], sha512[4:6], sha512)

	def get_object(self, hashes: dict):
		"""
		Returns a FastPullObject representing the object by cryptographic hash if it exists.

		``hashes`` is a dictionary which contains cryptographic hashes and optionally a filesize in "size",
		which will be used to match the requested file. ``hashes['sha512']`` must exist for the lookup to
		succeed, or a ``BLOSInvalidRequest`` will be raised. In addition, all fields defined in
		``self.req_client_hashes`` must exist, or ``BLOSInvalidRequest`` error will be raised.

		``get_object()`` performs by default an 'adequate' level of verification for object retrieval,
		though there are ways that this can be tuned.

		If integrity checks succeed, a reference to the object will be returned. If integrity checks fail,
		then a ``BLOSHashError`` will be raised with the details related to the expected and
		actual hashes in the exception itself.

		If the requested object is not found, BLOSNotFoundError will be raised.
		"""

		client_hash_names = set(hashes.keys())
		missing = client_hash_names - self.req_client_hashes
		if missing:
			raise BLOSInvalidRequest(f"Missing hashes in request: {missing}")

		index = hashes['sha512']
		exists_on_disk = False
		disk_path = self.get_disk_path(index)
		if os.path.exists(disk_path):
			exists_on_disk = True

		if not exists_on_disk:
			raise BLOSNotFoundError(f"Object does not exist on disk.")

		db_record = self.collection.findOne({"sha512": index})

		if not exists_on_disk and self.backfill in (BackFillStrategy.NONE, BackFillStrategy.DESIRED):
			raise BLOSNotFoundError(f"Object exists on disk but no DB record exists. Backfill strategy is {self.backfill}.")

		db_hash_names = set(db_record["hashes"].keys())
		missing_expected_client_hashes = self.req_blos_hashes - db_hash_names
		if self.backfill == BackFillStrategy.NONE and missing_expected_client_hashes:
			raise BLOSIncompleteRecord(f"BLOS record is missing digest: {missing_expected_client_hashes}")

		# If we have gotten here, we have validated that we have all the hashes we absolutely need from client.
		# We will now check that all our hashes are in harmony. This will take into account client-supplied
		# hashes, db record hashes, as well as any disk hashes we are required to check in real-time:

		common_hashes = client_hash_names & db_hash_names
		invalid_hashes = {}

		# This calculates all the hashes we need from the object on disk:
		disk_hashes = calc_hashes(disk_path, self.disk_verify)

		corrupt = False
		for hash_name in common_hashes:
			if hash_name in self.disk_verify:
				diskhash = disk_hashes["hashes"][hash_name]
			else:
				diskhash = None
			supplied = db_record["hashes"][hash_name]
			recorded = hashes[hash_name]
			if diskhash and (supplied == recorded == diskhash):
				# All hashes match!
				pass
			elif supplied == recorded:
				# We don't require disk hash checking, and supplied hash and recorded match!
				pass
			else:
				if diskhash and (recorded != diskhash):
					# Disk corruption or tampering!
					corrupt = True
				invalid_hashes[hash_name] = {
					"supplied": supplied,
					"recorded": recorded,
				}
				if diskhash:
					invalid_hashes[hash_name]["diskhash"] = diskhash

		if corrupt:
			os.unlink(disk_path)
			raise BLOSCorruptionError(invalid_hashes)
		elif invalid_hashes:
			raise BLOSHashError(invalid_hashes)

		# We will record any new hashes we do not yet have, which we have happened to have just calculated --
		# according to our backfill strategy:

		if self.backfill != BackFillStrategy.NONE:
			to_be_recorded_db_hashes = self.desired_hashes & disk_hashes
			if to_be_recorded_db_hashes:
				new_hash_dict = {}
				for hash in to_be_recorded_db_hashes:
					new_hash_dict[hash] = disk_hashes["hashes"][hash]

				if self.backfill == BackFillStrategy.ALL and not db_record:
					self.collection.insertOne({
						"hashes": new_hash_dict
					})
				else:
					self.collection.updateOne({"sha512": index}, {"hashes": new_hash_dict})

		# All done.
		return BLOSResponse(path=disk_path, checked_hashes=common_hashes)

	def insert_object(self, temp_path, pregenned_hashes=None):

		"""
		This will be used to directly add an object to fastpull, by pointing to the file to insert, and its
		final data. The final data (hashes) are optional. We will by default generate all missing final data.
		We will use self.desired_hashes as a reference of what we want.

		There is potential complication involved if the object already exists in our MongoDB collection --
		what do we do in this case? In this case, we will raise an BLOSObjectAlreadyExists exception and NOT
		insert the object. This just keeps this security-focused code as clean as possible.

		We will also throw this exception if the on-disk object already exists.

		We will perform a 'fixup' of any existing records only on object *retrieval*, not on object *insert*.
		So if there's an existing record that is missing some hashes that we would want to automatically add,
		we don't do that fixing-up here.

		Intentionally keeping this very simple.
		"""

		if pregenned_hashes is None:
			pregenned_hashes = {}

		pregenned_set = set(pregenned_hashes.keys())
		missing = pregenned_set - self.req_client_hashes
		if missing:
			raise BLOSInvalidRequest(f"Missing hashes in request: {missing}")

		index = pregenned_hashes['sha512']

		existing = self.collection.findOne({'hashes.sha512': index})
		if existing:
			raise BLOSObjectAlreadyExists("mongo db record already exists.")
		disk_path = self.get_disk_path(index)
		if os.path.exists(disk_path):
			raise BLOSObjectAlreadyExists(f"no mongo db record but disk file already exists: {disk_path}")

		try:
			os.link(temp_path, disk_path)
		except FileNotFoundError:
			raise BLOSNotFoundError(f"Source file {temp_path} not found.")
		except FileExistsError:
			# possible race? multiple threads inserting same download shouldn't really happen
			pass
		# protect against possible race that shouldn't happen: multiple threads inserting same download.
		self.collection.update_one({"hashes": pregenned_hashes}, upsert=True)

		def delete_object(hashes: dict):
			"""
			This method is used to delete objects from the BLOS. It shouldn't generally need to be used.
			"""
			pass