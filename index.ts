import { AbstractDatabase } from 'orm-js/abstract-database';
import { Schema, Table, Field } from 'orm-js/schema';
import { Query, EntityClass, ExpressionNode } from 'orm-js/query';
import sqlite = require('sqlite');


interface SqliteField {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  // dflt_value: null;
  pk: number;
}

let binaryOperations = {
  'eq': '=',
  'neq': '<>',
  'lt': '<', 'le': '<=',
  'gt': '>', 'ge': '>=',
  'or': 'OR', 'and': 'AND'
};


export default class SqliteDatabase extends AbstractDatabase {
  constructor(private fileName: string) {
    super();
  }
  
  async connect() {
    await sqlite.open(this.fileName);
  }
  
  async createSchema() {
    for(let table of this.schema.tables.values()) {
      if(await this.tableExists(table)) {
        let definedFields = await this.getDefinedFields(table);
        
        for(let field of table.fields.values()) {
          if(!definedFields.has(field.internalName)) {
            let query = `ALTER TABLE \`${table.name}\` ADD 
            \`${field.internalName}\``;
            await sqlite.run(query);
          }
        }
        
        for(let definedField of definedFields.values()) {
          if(!table.hasField(definedField.name)) {
            // remove column not supported by sqlite
            // todo: create new table with new structure
          }
        }
      }
      
      let fields = [];
      
      for(let field of table.fields.values()) {
        if(field.isArray) {
          // reference either by associated field or join table
          if(field.associatedField.isArray &&
            table.name < field.associatedField.table.name) {
            await this.createJoinTable(field);
          }
          
          continue;
        }
        
        let type = '';
        
        switch(field.type) {
          case String: type = 'TEXT'; break;
          case Number:
            if(field == table.idField) type = 'INTEGER';
            else type = 'REAL';
            break;
          case Boolean:
            type = 'INTEGER';
            break;
        }
        
        fields.push(`\`${field.internalName}\` ${type}`.trim());
      }
      
      let query = `CREATE TABLE \`${table.name}\` (${fields.join(', ')}`;
      if(table.idField) {
        query += `, PRIMARY KEY (\`${table.idField.internalName}\`)`;
      }
      
      query += `)`;
      
      await sqlite.run(query);
    }
  }
  
  private async createJoinTable(field: Field) {
    let tableName = field.joinTable;
    
    let fields = [
      `\`${field.table.name}_${field.table.idField.internalName}\``,
      `\`${field.associatedField.table.name}_${field.associatedField.table.idField.internalName}\``
    ];
    
    let query = `CREATE TABLE \`${tableName}\` (${fields.join(', ')})`;
    
    await sqlite.run(query);
  }
  
  private async tableExists(table: Table) {
    let query = `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${this.quote(table.name)}`;
    let result = await sqlite.all(query);
    
    return result.length > 0;
  }
  
  private async getDefinedFields(table: Table) {
    let query = `PRAGMA table_info(\`${table.name}\`)`;
    let result = <SqliteField[]>(await sqlite.all(query));
    
    let fields = new Map<string, SqliteField>();
    for(let field of result) {
      fields.set(field.name, field);
    }
    
    return fields;
  }
  
  async findAll<T>(query: Query, extra: Object[]) {
    let table = this.schema.getTable(query.target);
    
    let fields = this.selectFields(table, table.name, table.name).join(', ');
    
    if(query.aliases) {
      for(let alias of query.aliases) {
        fields += `, ${this.buildExpression(alias.node)} AS \`${alias.alias}\``;
      }
    }
    
    let stmt = `SELECT ${fields} FROM \`${table.name}\` ${this.buildQueryParams(query)}`;
    
    let data = await sqlite.all(stmt);
    
    let items = [];
    let idField = table.idField;
    
    for(let entry of data) {
      let item = this.schema.convertObjectToItem(entry, query.target, table.name);
      
      if(idField && items.length > 0) {
        /* joined data belonging to array of previous item */
        let lastItem = items[items.length - 1];
        if(lastItem[idField.name] == item[idField.name]) {
          for(let key in item) {
            if(lastItem[key] instanceof Array) {
              lastItem[key] = lastItem[key].concat(item[key]);
            }
          }
          
          continue;
        }
      }
      
      item = this.schema.convertObjectToItem(entry, query.target, table.name);
      items.push(item);
      
      if(extra && query.aliases) {
        let extraItem = {};
        
        for(let alias of query.aliases) {
          extraItem[alias.alias] = entry[alias.alias];
        }
        
        extra.push(extraItem);
      }
    }
    
    return items;
  }
  
  async insert(query: Query, item: Object) {
    let table = this.schema.getTable(query.target);
    
    if(table.extends) {
      let subQuery = <Query> {};
      
      for(let key in query) {
        subQuery[key] = query[key];
      }
      
      subQuery.target = <EntityClass> table.extends.entity;
      
      await this.insert(subQuery, item);
    }
    
    let values = [];
    
    for(let field of table.fields.values()) {
      let value = null;
      
      if(field.associatedField) {
        let refField = field.associatedField;
        let refTable = refField.table;
        
        
        if(field.isArray) {
          if(!field.associatedField.isArray) continue;
          if(!item[table.idField.name]) continue;
          
          let stmt = `DELETE FROM \`${field.joinTable}\` ` +
            `WHERE \`${table.name}_${table.idField.internalName}\` = `;
          
          stmt += this.quote(item[table.idField.name]);
          
          await sqlite.run(stmt);
          
          if(item[field.name]) {
            let fields = [
              `\`${table.name}_${table.idField.internalName}\``,
              `\`${refTable.name}_${refTable.idField.internalName}\``
            ];
            
            let values = item[field.name].map(refItem =>
              `(${this.quote(item[table.idField.name])}, ${this.quote(refItem[refTable.idField.name])})`
            );
            
            stmt = `INSERT INTO \`${field.joinTable}\` (${fields.join(', ')}) ` +
              `VALUES ${values.join(', ')}`;
            
            await sqlite.run(stmt);
          }
          
          continue;
        }
        
        
        if(!refTable.idField) {
          throw new Error(`Entity ${refTable.name} has no Id field.`);
        }
        
        if(!item[field.name]) {
          value = null;
        } else {
          let idField = refTable.idField.name;
          if(field.isArray && !refField.isArray) {
            // do nothing, mapping is done by refField
          } else if(field.isArray && refField.isArray) {
            // add assoc in an extra table (already done above)
          } else {
            value = item[field.name][idField];
          }
        }
      } else {
        value = item[field.name];
      }
      
      if(typeof value == 'number') values.push(value);
      else if(value == null) values.push('NULL');
      else values.push(`'${value}'`);
    }
    
    let fields = [];
    for(let field of table.fields.values()) {
      if(field.isArray) {
        // reference either by associated field or join table
        continue;
      }
      
      fields.push(`\`${field.internalName}\``);
    }
    
    let stmt = `INSERT OR REPLACE INTO \`${table.name}\` (${fields.join(', ')}) VALUES (${values.join(', ')})`;
    let result = await sqlite.run(stmt);
    
    if(table.idField) {
      if(result.lastID && !item[table.idField.name]) {
        item[table.idField.name] = result.lastID;
      }
    }
  }
  
  async delete(query: Query, item?: Object) {
    // todo
  }
  
  async total(query: Query) {
    let table = this.schema.getTable(query.target);
    let fields = this.selectFields(table, table.name, table.name).join(', ');
    // todo: do we really need to select additional fields here?
    
    let stmt = `SELECT COUNT(*) AS \`count\`, ${fields} FROM \`${table.name}\` ${this.buildQueryParams(query, false)}`;
    
    let result = await sqlite.get(stmt);
    return result['count'];
  }
  
  private selectFields(table: Table, alias: string, prefix: string, assocs = new Set<Field>()) {
    let selection = [];
    
    if(table.extends) {
      let subSelection = this.selectFields(table.extends,
        `${alias}._base`, `${prefix}._base`, assocs);
      selection = selection.concat(subSelection);
    }
    
    for(let field of table.fields.values()) {
      /*
       * Issue:
       * 
       *   B1
       *  /  \
       * A    C
       *  \  /
       *   B2
       * 
       * Target entity A has two fields associated with B (B1, B2).
       * Entity B has an associated field with C.
       * After select for B1 -> C was added,
       * B2 -> C won't be added, as the association B <-> C is marked as done.
       */
      if(field.associatedField) {
        if(!assocs.has(field.associatedField)) {
          assocs.add(field);
          
          let refTable = field.associatedField.table;
          let subSelection = this.selectFields(refTable, 
            `${prefix}.${field.internalName}`,
            `${prefix}.${field.internalName}`,
            assocs);
          selection = selection.concat(subSelection);
        }
      } else if(!field.isArray) {
        selection.push(`\`${alias}\`.\`${field.internalName}\` AS \`${prefix}.${field.internalName}\``);
      }
    }
    
    return selection;
  }
  
  private buildExpression(node: ExpressionNode) {
    let stmt = '';
    
    switch(node.type) {
      case 'literal':
        return this.quote(node.value);
      case 'field':
        {
          let field = <string> node.field;
          
          return ` \`${field}\``;
        }
      case 'eq': case 'neq':
      case 'lt': case 'le':
      case 'gt': case 'ge':
      case 'or': case 'and':
        {
          let operator = binaryOperations[node.type];
          
          stmt += ` ${this.buildExpression(node.left)} ${operator} ${this.buildExpression(node.right)}`;
          break;
        }
      case 'call':
        let params = [];
        
        for(let param of node.parameters) {
          if(param.type == 'field') {
            // functions can't access the aliases
            let match = /^(.+)\.([^.]+)$/.exec(param.field);
            params.push(`\`${match[1]}\`.\`${match[2]}\``);
          } else {
            params.push(this.buildExpression(param));
          }
        }
        
        stmt += ` ${node.function.toUpperCase()}(${params.join(', ')})`;
        break;
    }
    
    return `(${stmt.trim()})`;
  }
  
  private buildJoins(table: Table, alias: string, prefix: string, assocs = new Set<Field>()) {
    let stmt = '';
    
    if(table.extends) {
      let idField = table.idField.internalName;
      stmt += ` JOIN \`${table.extends.name}\` AS \`${prefix}._base\``;
      stmt += ` ON \`${alias}\`.\`${idField}\` = ` +
        `\`${prefix}._base\`.\`${idField}\``;
    }
    
    for(let field of table.fields.values()) {
      if(!field.associatedField || assocs.has(field.associatedField)) continue;
      assocs.add(field);
      
      let refField = field.associatedField;
      let refTable = refField.table;
      
      if(!field.isArray) {
        /* 1-1, 1-n */
        stmt += ` LEFT JOIN \`${refTable.name}\` AS \`${prefix}.${field.internalName}\``;
        stmt += ` ON \`${alias}\`.\`${field.internalName}\` = ` +
          `\`${prefix}.${field.internalName}\`.\`${refTable.idField.internalName}\``;
      } else if(!field.associatedField.isArray) {
        /* n-1 */
        stmt += ` LEFT JOIN \`${refTable.name}\` AS \`${prefix}.${field.internalName}\``;
        stmt += ` ON \`${alias}\`.\`${table.idField.internalName}\` = ` +
          `\`${prefix}.${field.internalName}\`.\`${refField.internalName}\``;
      } else {
        /* m-n */
        let idName = table.idField.internalName;
        let refIdName = `${table.name}_${table.idField.internalName}`;
        
        stmt += ` LEFT JOIN \`${field.joinTable}\` AS \`${prefix}_${field.internalName}\``;
        stmt += ` ON \`${alias}\`.\`${idName}\` = ` +
          `\`${prefix}_${field.internalName}\`.\`${refIdName}\``;
        
        idName = refTable.idField.internalName;
        refIdName = `${refTable.name}_${refTable.idField.internalName}`;
        stmt += ` LEFT JOIN \`${refTable.name}\` AS \`${prefix}.${field.internalName}\``;
        stmt += ` ON \`${prefix}.${field.internalName}\`.\`${idName}\` = ` +
          `\`${prefix}_${field.internalName}\`.\`${refIdName}\``;
      }
      
      stmt += this.buildJoins(refTable,
        `${alias}.${field.internalName}`,
        `${alias}.${field.internalName}`,
        assocs);
    }
    
    return stmt;
  }
  
  private buildQueryParams(query: Query, limit = true) {
    let stmt = '';
    
    let table = this.schema.getTable(query.target);
    stmt += this.buildJoins(table, table.name, table.name);
    
    if(query.condition) {
      stmt += ` WHERE ${this.buildExpression(query.condition)}`;
    }
    
    if(query.groups) {
      stmt += ' GROUP BY';
      
      let groups = [];
      for(let group of query.groups) {
        groups.push(this.buildExpression(group));
      }
      
      stmt += groups.join(', ');
    }
    
    if(query.orders) {
      stmt += ' ORDER BY';
      
      let orders = [];
      for(let order of query.orders) {
        let field = order.field;
        
        let snippet = ` \`${field}\``;
        
        if(order.order) {
          snippet += ` ${order.order.toUpperCase()}`;
        }
        
        orders.push(snippet);
      }
      
      stmt += orders.join(', ');
    }
    
    if(limit) {
      if(query.limit) {
        stmt += ` LIMIT ${query.limit}`;
        
        if(query.offset) {
          stmt += ` OFFSET ${query.offset}`;
        }
      }
    }
    
    return stmt;
  }
  
  private quote(value: any) {
    if(typeof value == 'number') return value.toString();
    else if(typeof value == 'bolean') return value ? '1' : '0';
    else if(value == null) return 'NULL'; // null and undefined
    else return `'${value.replace(/'/g, "''")}'`;
  }
}