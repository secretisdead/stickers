import uuid
import time
import re
from ipaddress import ip_address
from enum import Enum
from datetime import datetime, timezone

from sqlalchemy import Table, Column, PrimaryKeyConstraint, Binary as sqla_binary, Float
from sqlalchemy import Integer, String, MetaData, distinct
from sqlalchemy.dialects.mysql import VARBINARY as mysql_binary
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, and_, or_

from statement_helper import sort_statement, paginate_statement, id_filter
from statement_helper import time_cutoff_filter, string_like_filter
from statement_helper import string_equal_filter
from statement_helper import bitwise_filter
from idcollection import IDCollection
from parse_id import parse_id, get_id_bytes, generate_or_parse_id

class Sticker:
	def __init__(
			self,
			id=None,
			creation_time=None,
			name='',
			display='',
			category='',
			category_order=0,
			group_bits=0,
		):
		self.id, self.id_bytes = generate_or_parse_id(id)

		if None == creation_time:
			creation_time = time.time()
		self.creation_time = int(creation_time)
		self.creation_datetime = datetime.fromtimestamp(
			self.creation_time,
			timezone.utc,
		)

		self.name = str(name)
		self.display = str(display)
		self.category = str(category)
		self.category_order = int(category_order)

		if isinstance(group_bits, int):
			group_bits = group_bits.to_bytes(2, 'big')
		else:
			group_bits = bytes(group_bits)
		self.group_bits = group_bits

class CollectedSticker:
	def __init__(
			self,
			id=None,
			receive_time=None,
			user_id='',
			sticker_id='',
		):
		self.id, self.id_bytes = generate_or_parse_id(id)

		if None == receive_time:
			receive_time = time.time()
		self.receive_time = int(receive_time)
		self.receive_datetime = datetime.fromtimestamp(
			self.receive_time,
			timezone.utc,
		)

		self.user_id, self.user_id_bytes = parse_id(user_id)
		self.sticker_id, self.sticker_id_bytes = parse_id(sticker_id)

		self.sticker = None

class StickerPlacement:
	def __init__(
			self,
			id=None,
			placement_time=None,
			subject_id='',
			user_id='',
			sticker_id='',
			position_x=0.0,
			position_y=0.0,
			rotation=0.0,
			scale=0.0,
		):
		self.id, self.id_bytes = generate_or_parse_id(id)

		if None == placement_time:
			placement_time = time.time()
		self.placement_time = int(placement_time)
		self.placement_datetime = datetime.fromtimestamp(
			self.placement_time,
			timezone.utc,
		)

		self.subject_id, self.subject_id_bytes = generate_or_parse_id(subject_id)
		self.user_id, self.user_id_bytes = parse_id(user_id)
		self.sticker_id, self.sticker_id_bytes = parse_id(sticker_id)

		self.position_x = float(position_x)
		self.position_y = float(position_y)
		self.rotation = float(rotation)
		self.scale = float(scale)

		self.sticker = None

class Stickers:
	def __init__(self, engine, db_prefix='', install=False, connection=None):
		self.engine = engine
		self.engine_session = sessionmaker(bind=self.engine)()

		self.db_prefix = db_prefix

		self.name_length = 16
		self.display_length = 32
		self.category_length = 16

		metadata = MetaData()

		default_bytes = 0b0 * 16

		if 'mysql' == self.engine_session.bind.dialect.name:
			Binary = mysql_binary
		else:
			Binary = sqla_binary

		# stickers tables
		self.stickers = Table(
			self.db_prefix + 'stickers',
			metadata,
			Column('id', Binary(16), default=default_bytes),
			Column('creation_time', Integer, default=0),
			Column('name', String(self.name_length)),
			Column('display', String(self.display_length)),
			Column('category', String(self.category_length)),
			Column('category_order', Integer, default=0),
			Column('group_bits', Integer, default=0),
			PrimaryKeyConstraint('id'),
		)

		# collected stickers tables
		self.collected_stickers = Table(
			self.db_prefix + 'collected_stickers',
			metadata,
			Column('id', Binary(16), default=default_bytes),
			Column('receive_time', Integer, default=0),
			Column('user_id', Binary(16), default=default_bytes),
			Column('sticker_id', Binary(16), default=default_bytes),
			PrimaryKeyConstraint('id'),
		)

		# placed stickers tables
		self.sticker_placements = Table(
			self.db_prefix + 'sticker_placements',
			metadata,
			Column('id', Binary(16), default=default_bytes),
			Column('placement_time', Integer, default=0),
			Column('subject_id', Binary(16), default=default_bytes),
			Column('user_id', Binary(16), default=default_bytes),
			Column('sticker_id', Binary(16), default=default_bytes),
			Column('position_x', Float, default=0),
			Column('position_y', Float, default=0),
			Column('rotation', Float, default=0),
			Column('scale', Float, default=0),
			PrimaryKeyConstraint('id'),
		)

		if connection:
			self.connection = connection
		else:
			self.connection = self.engine.connect()

		if install:
			for table in [
					self.stickers,
					self.collected_stickers,
					self.sticker_placements,
				]:
				table.create(bind=self.engine, checkfirst=True)

	def uninstall(self):
		for table in [
				self.stickers,
				self.collected_stickers,
				self.sticker_placements,
			]:
			table.drop(self.engine)

	# retrieve stickers
	def get_sticker(self, id):
		stickers = self.search_stickers(filter={'ids': id})
		return stickers.get(id)

	def prepare_stickers_search_statement(self, filter):
		conditions = []
		conditions += id_filter(filter, 'ids', self.stickers.c.id)
		conditions += time_cutoff_filter(
			filter,
			'created',
			self.stickers.c.creation_time,
		)
		conditions += string_like_filter(
			filter,
			'name',
			self.stickers.c.name,
		)
		conditions += string_like_filter(
			filter,
			'display',
			self.stickers.c.display,
		)
		conditions += string_equal_filter(
			filter,
			'category',
			self.stickers.c.category,
		)
		conditions += bitwise_filter(
			filter,
			'group_bits',
			self.stickers.c.group_bits,
		)

		statement = self.stickers.select()
		if conditions:
			statement = statement.where(and_(*conditions))
		return statement

	def count_stickers(self, filter={}):
		statement = self.prepare_stickers_search_statement(filter)
		statement = statement.with_only_columns(
			[func.count(self.stickers.c.id)]
		)
		return self.connection.execute(statement).fetchone()[0]

	def search_stickers(
			self,
			filter={},
			sort='',
			order='',
			page=0,
			perpage=None
		):
		statement = self.prepare_stickers_search_statement(filter)

		statement = sort_statement(
			statement,
			self.stickers,
			sort,
			order,
			'creation_time',
			True,
			[
				'creation_time',
				'id',
			],
		)
		statement = paginate_statement(statement, page, perpage)

		result = self.connection.execute(statement).fetchall()
		if 0 == len(result):
			return IDCollection()

		stickers = IDCollection()
		for row in result:
			sticker = Sticker(
				id=row[self.stickers.c.id],
				creation_time=row[self.stickers.c.creation_time],
				name=row[self.stickers.c.name],
				display=row[self.stickers.c.display],
				category=row[self.stickers.c.category],
				category_order=row[self.stickers.c.category_order],
				group_bits=row[self.stickers.c.group_bits],
			)

			stickers.add(sticker)
		return stickers

	# manipulate stickers
	def create_sticker(self, **kwargs):
		sticker = Sticker(**kwargs)
		# preflight check for existing id
		if self.count_stickers(filter={'ids': sticker.id_bytes}):
			raise ValueError('Sticker ID collision')
		self.connection.execute(
			self.stickers.insert(),
			id=sticker.id_bytes,
			creation_time=int(sticker.creation_time),
			name=str(sticker.name),
			display=str(sticker.display),
			category=str(sticker.category),
			category_order=int(sticker.category_order),
			group_bits=int.from_bytes(sticker.group_bits, 'big'),
		)
		return sticker

	def update_sticker(self, id, **kwargs):
		sticker = Sticker(id=id, **kwargs)
		updates = {}
		if 'creation_time' in kwargs:
			updates['creation_time'] = int(sticker.creation_time)
		if 'name' in kwargs:
			updates['name'] = str(sticker.name)
		if 'display' in kwargs:
			updates['display'] = str(sticker.display)
		if 'category' in kwargs:
			updates['category'] = str(sticker.category)
		if 'category_order' in kwargs:
			updates['category_order'] = int(sticker.category_order)
		if 'group_bits' in kwargs:
			updates['group_bits'] = int.from_bytes(sticker.group_bits, 'big')
		if 0 == len(updates):
			return
		self.connection.execute(
			self.stickers.update().values(**updates).where(
				self.stickers.c.id == sticker.id_bytes
			)
		)

	def delete_sticker(self, id):
		id = get_id_bytes(id)
		self.connection.execute(
			self.collected_stickers.delete().where(
				self.collected_stickers.c.id == id
			)
		)
		self.connection.execute(
			self.sticker_placements.delete().where(
				self.sticker_placements.c.id == id
			)
		)
		self.connection.execute(
			self.stickers.delete().where(self.stickers.c.id == id)
		)

	# retrieve collected stickers
	def get_collected_sticker(self, id):
		collected_stickers = self.search_collected_stickers(filter={'ids': id})
		return collected_stickers.get(id)

	def prepare_collected_stickers_search_statement(self, filter):
		conditions = []
		conditions += id_filter(filter, 'ids', self.collected_stickers.c.id)
		conditions += time_cutoff_filter(
			filter,
			'received',
			self.collected_stickers.c.receive_time,
		)
		conditions += id_filter(
			filter,
			'user_ids',
			self.collected_stickers.c.user_id,
		)
		conditions += id_filter(
			filter,
			'sticker_ids',
			self.collected_stickers.c.sticker_id,
		)

		statement = self.collected_stickers.select()
		if conditions:
			statement = statement.where(and_(*conditions))
		return statement

	def count_collected_stickers(self, filter={}):
		statement = self.prepare_collected_stickers_search_statement(filter)
		statement = statement.with_only_columns(
			[func.count(self.collected_stickers.c.id)]
		)
		return self.connection.execute(statement).fetchone()[0]

	def search_collected_stickers(
			self,
			filter={},
			sort='',
			order='',
			page=0,
			perpage=None
		):
		statement = self.prepare_collected_stickers_search_statement(filter)

		statement = sort_statement(
			statement,
			self.collected_stickers,
			sort,
			order,
			'receive_time',
			True,
			[
				'receive_time',
				'id',
			],
		)
		statement = paginate_statement(statement, page, perpage)

		result = self.connection.execute(statement).fetchall()
		if 0 == len(result):
			return IDCollection()

		sticker_ids = []
		for row in result:
			sticker_ids.append(row[self.collected_stickers.c.sticker_id])

		stickers = self.search_stickers(filter={'sticker_ids': sticker_ids})

		collected_stickers = IDCollection()
		for row in result:
			collected_sticker = CollectedSticker(
				id=row[self.collected_stickers.c.id],
				receive_time=row[self.collected_stickers.c.receive_time],
				user_id=row[self.collected_stickers.c.user_id],
				sticker_id=row[self.collected_stickers.c.sticker_id],
			)
			if collected_sticker.sticker_id_bytes in stickers:
				collected_sticker.sticker = stickers.get(
					collected_sticker.sticker_id_bytes
				)

			collected_stickers.add(collected_sticker)
		return collected_stickers

	# manipulate collected stickers
	def grant_sticker(self, sticker_id, user_id, receive_time=None):
		sticker_id = get_id_bytes(sticker_id)
		user_id = get_id_bytes(user_id)
		collected_stickers = self.search_collected_stickers(
			filter={'user_ids': user_id, 'sticker_ids': sticker_id},
		)
		if 0 < len(collected_stickers):
			raise ValueError('Specified user already has the specified sticker')
		collected_sticker = CollectedSticker(
			user_id=user_id,
			sticker_id=sticker_id,
			receive_time=receive_time,
		)
		if self.count_collected_stickers(filter={'ids': collected_sticker.id_bytes}):
			raise ValueError('Collected sticker ID collision')
		self.connection.execute(
			self.collected_stickers.insert(),
			id=collected_sticker.id_bytes,
			receive_time=int(collected_sticker.receive_time),
			user_id=collected_sticker.user_id_bytes,
			sticker_id=collected_sticker.sticker_id_bytes,
		)
		return collected_sticker

	def revoke_sticker(self, id):
		id = get_id_bytes(id)
		self.connection.execute(
			self.collected_stickers.delete().where(
				self.collected_stickers.c.id == id,
			)
		)

	def get_collected_stickers(self, user_id):
		return self.search_collected_stickers(filter={'user_ids': user_id})

	# retrieve sticker placements
	def get_sticker_placement(self, id):
		sticker_placements = self.search_sticker_placements(filter={'ids': id})
		return sticker_placements.get(id)

	def prepare_sticker_placements_search_statement(self, filter):
		conditions = []
		conditions += id_filter(filter, 'ids', self.sticker_placements.c.id)
		conditions += time_cutoff_filter(
			filter,
			'placed',
			self.sticker_placements.c.placement_time,
		)
		conditions += id_filter(
			filter,
			'subject_ids',
			self.sticker_placements.c.subject_id,
		)
		conditions += id_filter(
			filter,
			'user_ids',
			self.sticker_placements.c.user_id,
		)
		conditions += id_filter(
			filter,
			'sticker_ids',
			self.sticker_placements.c.sticker_id,
		)

		statement = self.sticker_placements.select()
		if conditions:
			statement = statement.where(and_(*conditions))
		return statement

	def count_sticker_placements(self, filter={}):
		statement = self.prepare_sticker_placements_search_statement(filter)
		statement = statement.with_only_columns(
			[func.count(self.sticker_placements.c.id)]
		)
		return self.connection.execute(statement).fetchone()[0]

	def search_sticker_placements(
			self,
			filter={},
			sort='',
			order='',
			page=0,
			perpage=None
		):
		statement = self.prepare_sticker_placements_search_statement(filter)

		statement = sort_statement(
			statement,
			self.sticker_placements,
			sort,
			order,
			'placement_time',
			True,
			[
				'placement_time',
				'id',
			],
		)
		statement = paginate_statement(statement, page, perpage)

		result = self.connection.execute(statement).fetchall()
		if 0 == len(result):
			return IDCollection()

		sticker_ids = []
		for row in result:
			sticker_ids.append(row[self.sticker_placements.c.sticker_id])

		stickers = self.search_stickers(filter={'sticker_ids': sticker_ids})

		sticker_placements = IDCollection()
		for row in result:
			sticker_placement = StickerPlacement(
				id=row[self.sticker_placements.c.id],
				placement_time=row[self.sticker_placements.c.placement_time],
				subject_id=row[self.sticker_placements.c.subject_id],
				user_id=row[self.sticker_placements.c.user_id],
				sticker_id=row[self.sticker_placements.c.sticker_id],
				position_x=row[self.sticker_placements.c.position_x],
				position_y=row[self.sticker_placements.c.position_y],
				rotation=row[self.sticker_placements.c.rotation],
				scale=row[self.sticker_placements.c.scale],
			)
			if sticker_placement.sticker_id_bytes in stickers:
				sticker_placement.sticker = stickers.get(
					sticker_placement.sticker_id_bytes
				)

			sticker_placements.add(sticker_placement)
		return sticker_placements

	# manipulate sticker placements
	def place_sticker(self, **kwargs):
		sticker_placement = StickerPlacement(**kwargs)
		self.connection.execute(
			self.sticker_placements.insert(),
			id=sticker_placement.id_bytes,
			placement_time=int(sticker_placement.placement_time),
			subject_id=sticker_placement.subject_id_bytes,
			user_id=sticker_placement.user_id_bytes,
			sticker_id=sticker_placement.sticker_id_bytes,
			position_x=float(sticker_placement.position_x),
			position_y=float(sticker_placement.position_y),
			rotation=float(sticker_placement.rotation),
			scale=float(sticker_placement.scale),
		)
		return sticker_placement

	def unplace_sticker(self, id):
		id = get_id_bytes(id)
		self.connection.execute(
			self.sticker_placements.delete().where(
				self.sticker_placements.c.id == id
			)
		)

	#TODO tests
	def prune_user_sticker_placements(self, subject_id, user_id, maximum_stickers):
		try:
			subject_id = get_id_bytes(subject_id)
		#TODO narrow catch
		except:
			return
		try:
			user_id = get_id_bytes(user_id)
		#TODO narrow catch
		except:
			return
		placements = self.search_sticker_placements(
			filter={
				'subject_ids': subject_id,
				'user_ids': user_id,
			},
			sort='placement_time',
			order='desc',
		)
		conditions = []
		i = 0
		for placement in placements.values():
			i += 1
			if i < maximum_stickers:
				continue
			conditions.append(self.sticker_placements.c.id == placement.id_bytes)
		if not conditions:
			return
		statement = self.sticker_placements.delete().where(
			and_(
				self.sticker_placements.c.subject_id == subject_id,
				self.sticker_placements.c.user_id == user_id,
				or_(*conditions),
			)
		)
		self.connection.execute(statement)

	#TODO tests
	def unplace_by_user(self, user_id):
		try:
			user_id = get_id_bytes(user_id)
		#TODO narrow catch
		except:
			return
		self.connection.execute(
			self.sticker_placements.delete().where(
				self.sticker_placements.c.user_id == user_id
			)
		)

	# unique categories
	def get_unique_categories(self):
		statement = self.stickers.select().with_only_columns(
			[self.stickers.c.category]
		).group_by(self.stickers.c.category)
		result = self.engine.execute(statement).fetchall()
		unique_categories = []
		for row in result:
			unique_categories.append(row[self.stickers.c.category])
		return unique_categories

	#TODO tests
	def get_user_unique_sticker_placement_counts(self, user_id):
		user_id = get_id_bytes(user_id)
		statement = self.sticker_placements.select().where(
			self.sticker_placements.c.user_id == user_id
		).with_only_columns(
			[
				self.sticker_placements.c.sticker_id,
				func.count(distinct(self.sticker_placements.c.subject_id)),
			]
		).group_by(
			self.sticker_placements.c.sticker_id
		)
		result = self.connection.execute(statement).fetchall()
		unique_sticker_placement_counts = {}
		for row in result:
			sticker_id, count = row
			sticker_id, sticker_id_bytes = parse_id(sticker_id)
			unique_sticker_placement_counts[sticker_id] = count
		return unique_sticker_placement_counts

	#TODO tests
	def get_subject_sticker_placement_counts(self, subject_ids):
		if list != type(subject_ids):
			subject_ids = [subject_ids]
		conditions = []
		for subject_id in subject_ids:
			subject_id, subject_id_bytes = parse_id(subject_id)
			conditions.append(
				self.sticker_placements.c.subject_id == subject_id_bytes
			)
		statement = self.sticker_placements.select().where(
			or_(*conditions)
		).with_only_columns(
			[
				self.sticker_placements.c.subject_id,
				func.count(self.sticker_placements.c.id),
			]
		).group_by(
			self.sticker_placements.c.subject_id
		)
		result = self.connection.execute(statement).fetchall()
		subject_sticker_placement_counts = {}
		for row in result:
			subject_id, count = row
			subject_id, subject_id_bytes = parse_id(subject_id)
			subject_sticker_placement_counts[subject_id] = count
		return subject_sticker_placement_counts

	# anonymization
	def anonymize_id(self, id, new_id=None):
		id = get_id_bytes(id)

		if not new_id:
			new_id = uuid.uuid4().bytes

		self.connection.execute(
			self.collected_stickers.update().values(user_id=new_id).where(
				self.collected_stickers.c.user_id == id,
			)
		)
		self.connection.execute(
			self.sticker_placements.update().values(user_id=new_id).where(
				self.sticker_placements.c.user_id == id,
			)
		)

		return new_id
